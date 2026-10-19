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

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_RELEASE_REQ;

/**
 * LOCK_RELEASE — освобождение блокировки владельцем.
 * После успешного освобождения запись удаляется, и реплицируется как {@code deleted = true}.
 */
public final class LockReleaseOp extends AbstractLockCommon {

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

    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank()) {
      writeBadRequest(ctx, requestId);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      writeUnavailable(ctx, requestId);
      return;
    }

    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> locks.release(name, lockedBy),
              ApiProtocol.ST_OK,
              true)  // release удаляет запись → репликация как deleted
          .whenComplete((r, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              writeUnavailable(ctx, requestId);
              return;
            }
            writeLockResponse(ctx, r.status(), requestId, r.current());
          }));
      return;
    }

    Node primary = executor.primaryFor(name);
    tokenPeers.forwardLockRelease(primary, name, lockedBy)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeUnavailable(ctx, requestId);
            return;
          }
          writeLockResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockRelease(status);
  }
}
