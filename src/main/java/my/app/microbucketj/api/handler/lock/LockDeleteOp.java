package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.token.TokenPeerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;

/**
 * LOCK_DELETE — административное удаление блокировки (без проверки владельца).
 * Реплицируется как {@code deleted = true}.
 */
public final class LockDeleteOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockDeleteOp.class);

  private final LockExecutor executor;
  private final TokenPeerClient tokenPeers;

  public LockDeleteOp(LockExecutor executor, TokenPeerClient tokenPeers) {
    this.executor = Objects.requireNonNull(executor);
    this.tokenPeers = Objects.requireNonNull(tokenPeers);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_DELETE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_GET_REQ) return;  // requestId + nameLen
    int requestId = ApiLockCodec.readRequestId(in);
    String name = ApiLockCodec.readLockName(in);

    LOG.debug("Получен запрос LOCK_DELETE: reqId={} name={}", requestId, name);

    if (name == null || name.isBlank()) {
      LOG.debug("Некорректные параметры в запросе LOCK_DELETE: reqId={}", requestId);
      writeSimpleBadRequest(ctx, requestId);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      LOG.debug("Отказ в LOCK_DELETE (reqId={}): узел не лидер в одиночном режиме", requestId);
      writeSimpleUnavailable(ctx, requestId);
      return;
    }

    // Мы — primary: выполняем локально + реплицируем
    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> new LockRegistry.LockResult(locks.delete(name), null),
              ApiProtocol.ST_OK,
              true)  // delete удаляет запись → репликация как deleted
          .whenComplete((res, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              LOG.debug("Ошибка при репликации LOCK_DELETE: reqId={} lock={} error={}", requestId, name, err.getMessage());
              writeSimpleUnavailable(ctx, requestId);
              return;
            }

            LOG.debug("Успешная репликация LOCK_DELETE: reqId={} lock={} status={}", requestId, name, res.status());
            writeSimpleResponse(ctx, res.status(), requestId);
          }));
      return;
    }

    // Мы не primary: форвардим на первичный узел
    Node primary = executor.primaryFor(name);
    LOG.debug("Перенаправление LOCK_DELETE на primary: reqId={} lock={} primary={}", requestId, name, primary.id());
    tokenPeers.forwardLockDelete(primary, name)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null) {
            LOG.debug("Ошибка при перенаправлении LOCK_DELETE: reqId={} lock={} error={}", requestId, name, err.getMessage());
            writeSimpleUnavailable(ctx, requestId);
            return;
          }

          if (res == null) {
            LOG.debug("Получен null-ответ при перенаправлении LOCK_DELETE: reqId={} lock={}", requestId, name);
            writeSimpleUnavailable(ctx, requestId);
            return;
          }

          if (res.unavailable()) {
            LOG.debug("Primary вернул UNAVAILABLE при перенаправлении LOCK_DELETE: reqId={} lock={} primary={}",
                requestId, name, primary.id());
            writeSimpleUnavailable(ctx, requestId);
            return;
          }

          LOG.debug("Успешное перенаправление LOCK_DELETE: reqId={} lock={} status={} primary={}",
              requestId, name, res.status(), primary.id());
          writeSimpleResponse(ctx, res.status(), requestId);
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockDelete(status);
  }
}
