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

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;

public final class LockGetOp extends AbstractLockCommon {

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

    if (name == null || name.isBlank()) {
      writeLockGetResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    if (executor.isSingleNodeNonLeader()) {
      writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      return;
    }

    if (executor.isSelfPrimary(name)) {
      LockEntry cur = executor.locks().get(name);
      if (cur == null) {
        writeLockGetResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
      } else {
        writeLockGetResponse(ctx, ApiProtocol.ST_OK, requestId, cur);
      }
      return;
    }

    // forward на primary
    Node primary = executor.primaryFor(name);
    tokenPeers.forwardLockGet(primary, name)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }
          writeLockGetResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockGet(status);
  }
}
