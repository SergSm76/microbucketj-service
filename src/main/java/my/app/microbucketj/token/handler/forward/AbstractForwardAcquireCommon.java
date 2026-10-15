package my.app.microbucketj.token.handler.forward;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardResponseCodec;
import my.app.microbucketj.replication.ReplicationService;

import java.util.Objects;

public abstract class AbstractForwardAcquireCommon {

  protected final BucketRegistry buckets;
  private final MembershipRuntime membership;
  private final ReplicationService replication;

  AbstractForwardAcquireCommon(MembershipRuntime membership,
                       BucketRegistry buckets,
                       ReplicationService replication) {
    this.membership = Objects.requireNonNull(membership, "membership");
    this.buckets = Objects.requireNonNull(buckets, "buckets");
    this.replication = Objects.requireNonNull(replication, "replication");
  }

  protected void run(ChannelHandlerContext ctx, int requestId, long permits, long bucketHash, AbstractBucketRuntime rtLocal) {
    doRun(ctx, requestId, permits, bucketHash, rtLocal);
  }

  private void doRun(ChannelHandlerContext ctx,
                     int requestId,
                     long permits,
                     long bucketHash,
                     AbstractBucketRuntime rtLocal) {
    var snap = membership.snapshot();
    ClusterView view = snap.view();
    Node primary = view.pickPrimary(bucketHash);
    if (!primary.id().equals(view.self().id())) {
      writeFwdResp(ctx, TokenProtocol.ST_UNAVAILABLE, requestId, 0, 10);
      return;
    }

    AbstractBucketRuntime.AcquireResult ar;
    try {
      ar = rtLocal.acquireAsPrimary(permits);
    } catch (Exception e) {
      writeFwdResp(ctx, TokenProtocol.ST_UNAVAILABLE, requestId, 0, 10);
      return;
    }

    if (!ar.mutated()) {
      writeFwdResp(ctx, ar.status(), requestId, ar.remaining(), ar.retryAfterMs());
      return;
    }

    var replF = replication.replicatePrimaryMutation(view, snap.epoch(), bucketHash, rtLocal);
    if (replF.isDone()) {
      boolean ok = replF.getNow(false);
      writeFwdResp(ctx, ok ? ar.status() : TokenProtocol.ST_UNAVAILABLE, requestId, ar.remaining(), ar.retryAfterMs());
      return;
    }

    replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
      if (err != null || !ok) writeFwdResp(ctx, TokenProtocol.ST_UNAVAILABLE, requestId, 0, 10);
      else writeFwdResp(ctx, ar.status(), requestId, ar.remaining(), ar.retryAfterMs());
    }));
  }

  protected void writeFwdResp(ChannelHandlerContext ctx, byte status, int requestId, long remaining, long retryMs) {
    TokenForwardResponseCodec.writeFramed(ctx, status, requestId, remaining, retryMs);
  }
}
