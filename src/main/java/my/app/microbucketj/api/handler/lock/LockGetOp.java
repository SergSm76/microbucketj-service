package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.Objects;
import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;
import static my.app.microbucketj.utils.Hashing.fnv1a64;

public final class LockGetOp implements OpcodeHandler {

  private final BooleanSupplier isLeader;
  private final MembershipRuntime membership;
  private final LockRegistry locks;
  private final TokenPeerClient tokenPeers;

  public LockGetOp(BooleanSupplier isLeader,
                   MembershipRuntime membership,
                   LockRegistry locks,
                   TokenPeerClient tokenPeers) {
    this.isLeader = (isLeader == null) ? (() -> false) : isLeader;
    this.membership = Objects.requireNonNull(membership);
    this.locks = Objects.requireNonNull(locks);
    this.tokenPeers = Objects.requireNonNull(tokenPeers);
  }

  @Override public byte opcode() { return ApiProtocol.OP_LOCK_GET; }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_GET_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);
    String name = ApiLockCodec.readLockName(in);

    if (name == null || name.isBlank()) {
      recordMetric(ApiProtocol.ST_BAD_REQUEST);
      ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    long lockHash = fnv1a64(name);
    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      recordMetric(ApiProtocol.ST_UNAVAILABLE);
      ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (primary.id().equals(view.self().id())) {
      LockEntry cur = locks.get(name);
      if (cur == null) {
        recordMetric(ApiProtocol.ST_NOT_FOUND);
        ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
      } else {
        recordMetric(ApiProtocol.ST_OK);
        ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_OK, requestId, cur);
      }
      return;
    }

    // forward на primary
    tokenPeers.forwardLockGet(primary, name)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            recordMetric(ApiProtocol.ST_UNAVAILABLE);
            ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }
          recordMetric(res.status());
          ApiLockCodec.writeLockGetResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  private void recordMetric(byte status) {
    var m = MetricsHolder.get();
    if (m != null) m.recordLockGet(status);
  }
}
