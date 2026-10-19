package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
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

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;

public final class LockGetOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockGetOp.class);

  private final LockExecutor executor;
  private final TokenPeerClient tokenPeers;

  public LockGetOp(LockExecutor executor, TokenPeerClient tokenPeers) {
    this.executor = Objects.requireNonNull(executor);
    this.tokenPeers = Objects.requireNonNull(tokenPeers);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_GET;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_GET_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);
    String name = ApiLockCodec.readLockName(in);

    LOG.debug("Получен запрос LOCK_GET: reqId={} name={}", requestId, name);

    if (name == null || name.isBlank()) {
      LOG.debug("Некорректные параметры в запросе LOCK_GET: reqId={}", requestId);
      writeLockGetResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      LOG.debug("Отказ в LOCK_GET (reqId={}): узел не лидер в одиночном режиме", requestId);
      writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      return;
    }

    // Мы — primary: выполняем локально + реплицируем
    if (executor.isSelfPrimary(name)) {
      LockEntry cur = executor.locks().get(name);
      if (cur == null) {
        writeLockGetResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
      } else {
        writeLockGetResponse(ctx, ApiProtocol.ST_OK, requestId, cur);
      }
      return;
    }

    // Мы не primary: форвардим на первичный узел
    Node primary = executor.primaryFor(name);
    LOG.debug("Перенаправление LOCK_GET на primary: reqId={} lock={} primary={}", requestId, name, primary.id());
    tokenPeers.forwardLockGet(primary, name)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null) {
            LOG.debug("Ошибка при перенаправлении LOCK_GET: reqId={} lock={} error={}", requestId, name, err.getMessage());
            writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }

          if (res == null) {
            LOG.debug("Получен null-ответ при перенаправлении LOCK_GET: reqId={} lock={}", requestId, name);
            writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }

          if (res.unavailable()) {
            LOG.debug("Primary вернул UNAVAILABLE при перенаправлении LOCK_GET: reqId={} lock={} primary={}",
                requestId, name, primary.id());
            writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }

          LOG.debug("Успешное перенаправление LOCK_GET: reqId={} lock={} status={} primary={}",
              requestId, name, res.status(), primary.id());
          writeLockGetResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockGet(status);
  }
}
