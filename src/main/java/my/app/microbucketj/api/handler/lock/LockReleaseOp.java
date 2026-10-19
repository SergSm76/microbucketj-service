package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.token.TokenPeerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_RELEASE_REQ;

/**
 * LOCK_RELEASE — освобождение блокировки владельцем.
 * После успешного освобождения запись удаляется, и реплицируется как {@code deleted = true}.
 */
public final class LockReleaseOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockReleaseOp.class);

  private final LockExecutor executor;
  private final TokenPeerClient tokenPeers;

  public LockReleaseOp(LockExecutor executor, TokenPeerClient tokenPeers) {
    this.executor = Objects.requireNonNull(executor);
    this.tokenPeers = Objects.requireNonNull(tokenPeers);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_RELEASE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_RELEASE_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);
    String lockedBy = (name == null) ? null : ApiLockCodec.readLockedBy(in);

    LOG.debug("Получен запрос LOCK_RELEASE: reqId={} name={} lockedBy={}", requestId, name, lockedBy);

    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank()) {
      LOG.debug("Некорректные параметры в запросе LOCK_RELEASE: reqId={}", requestId);
      writeBadRequest(ctx, requestId);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      LOG.debug("Отказ в LOCK_RELEASE (reqId={}): узел не лидер в одиночном режиме", requestId);
      writeUnavailable(ctx, requestId);
      return;
    }

    // Мы — primary: выполняем локально + реплицируем
    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> locks.release(name, lockedBy),
              ApiProtocol.ST_OK,
              true)  // release удаляет запись → репликация как deleted
          .whenComplete((res, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              LOG.debug("Ошибка при репликации LOCK_RELEASE: reqId={} lock={} error={}", requestId, name, err.getMessage());
              writeUnavailable(ctx, requestId);
              return;
            }

            LOG.debug("Успешная репликация LOCK_RELEASE: reqId={} lock={} status={}", requestId, name, res.status());
            writeLockResponse(ctx, res.status(), requestId, res.current());
          }));
      return;
    }

    // Мы не primary: форвардим на первичный узел
    Node primary = executor.primaryFor(name);
    LOG.debug("Перенаправление LOCK_RELEASE на primary: reqId={} lock={} primary={}", requestId, name, primary.id());
    tokenPeers.forwardLockRelease(primary, name, lockedBy)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null) {
            LOG.debug("Ошибка при перенаправлении LOCK_RELEASE: reqId={} lock={} error={}", requestId, name, err.getMessage());
            writeUnavailable(ctx, requestId);
            return;
          }

          if (res == null) {
            LOG.debug("Получен null-ответ при перенаправлении LOCK_RELEASE: reqId={} lock={}", requestId, name);
            writeUnavailable(ctx, requestId);
            return;
          }

          if (res.unavailable()) {
            LOG.debug("Primary вернул UNAVAILABLE при перенаправлении LOCK_RELEASE: reqId={} lock={} primary={}",
                requestId, name, primary.id());
            writeUnavailable(ctx, requestId);
            return;
          }

          LOG.debug("Успешное перенаправление LOCK_RELEASE: reqId={} lock={} status={} primary={}",
              requestId, name, res.status(), primary.id());
          writeLockResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockRelease(status);
  }
}
