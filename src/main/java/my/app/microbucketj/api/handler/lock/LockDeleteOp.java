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

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;

/**
 * LOCK_DELETE — административное удаление блокировки (без проверки владельца).
 * Реплицируется как {@code deleted = true}.
 */
public final class LockDeleteOp extends AbstractLockCommon {

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

    if (name == null || name.isBlank()) {
      writeSimpleBadRequest(ctx, requestId);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      writeSimpleUnavailable(ctx, requestId);
      return;
    }

    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> new LockRegistry.LockResult(locks.delete(name), null),
              ApiProtocol.ST_OK,
              true)  // delete удаляет запись → репликация как deleted
          .whenComplete((r, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              writeSimpleUnavailable(ctx, requestId);
              return;
            }
            writeSimpleResponse(ctx, r.status(), requestId);
          }));
      return;
    }

    Node primary = executor.primaryFor(name);
    tokenPeers.forwardLockDelete(primary, name)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeSimpleUnavailable(ctx, requestId);
            return;
          }
          writeSimpleResponse(ctx, res.status(), requestId);
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockDelete(status);
  }
}
