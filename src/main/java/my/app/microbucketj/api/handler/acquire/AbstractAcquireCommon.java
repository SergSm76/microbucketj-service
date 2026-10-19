package my.app.microbucketj.api.handler.acquire;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.api.ApiResponseCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.Objects;
import java.util.function.BooleanSupplier;

public abstract class AbstractAcquireCommon implements OpcodeHandler {
  private static final long REMAINING_ERROR = 0;
  private static final long RETRY_AFTER_MS_ERROR = 10;
  private static final long RETRY_AFTER_MS_NOT_FOUND = 0;

  private final BooleanSupplier isLeader;
  private final MembershipRuntime membership;
  private final BucketRegistry buckets;
  private final TokenPeerClient tokenPeers;
  private final ReplicationService replication;

  protected AbstractAcquireCommon(BooleanSupplier isLeader,
                                  MembershipRuntime membership,
                                  BucketRegistry buckets,
                                  TokenPeerClient tokenPeers,
                                  ReplicationService replication) {
    this.isLeader = (isLeader == null) ? (() -> false) : isLeader;
    this.membership = Objects.requireNonNull(membership, "membership");
    this.buckets = Objects.requireNonNull(buckets, "buckets");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
    this.replication = Objects.requireNonNull(replication, "replication");
  }

  protected void runAcquire(ChannelHandlerContext ctx, int clientRequestId, long permits, long bucketHash) {
    runAcquire(ctx, clientRequestId, permits, bucketHash, null);
  }

  protected void runAcquire(ChannelHandlerContext ctx,
                            int clientRequestId,
                            long permits,
                            long bucketHash,
                            String bucketName) {
    if (bucketHash == Long.MIN_VALUE) {
      writeUnavailableError(ctx, clientRequestId);
      return;
    }

    permits = (permits <= 0) ? 1 : permits;

    AbstractBucketRuntime local = (bucketName != null) ? buckets.get(bucketName) : buckets.getByHashUnique(bucketHash);
    if (local == null) {
      if (bucketName == null && buckets.isCollisionHash(bucketHash)) writeCollisionError(ctx, clientRequestId);
      else writeNotFoundError(ctx, clientRequestId);
      return;
    }

    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      writeUnavailableError(ctx, clientRequestId);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(bucketHash);

    if (primary.id().equals(view.self().id())) {
      AbstractBucketRuntime.AcquireResult ar;
      try {
        ar = local.acquireAsPrimary(permits);
      } catch (Exception e) {
        writeUnavailableError(ctx, clientRequestId);
        return;
      }

      if (!ar.mutated()) {
        writeResp(ctx, ar.status(), clientRequestId, ar.remaining(), ar.retryAfterMs());
        return;
      }

      var replF = replication.replicatePrimaryMutation(view, snap.epoch(), bucketHash, local);
      if (replF.isDone()) {
        boolean ok = replF.getNow(false);
        if (!ok) writeUnavailableError(ctx, clientRequestId);
        else writeResp(ctx, ar.status(), clientRequestId, ar.remaining(), ar.retryAfterMs());
        return;
      }

      replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
        if (err != null || !ok) writeUnavailableError(ctx, clientRequestId);
        else writeResp(ctx, ar.status(), clientRequestId, ar.remaining(), ar.retryAfterMs());
      }));
      return;
    }

    (bucketName != null
        ? tokenPeers.forwardAcquire(primary, permits, bucketName)
        : tokenPeers.forwardAcquireByHash(primary, permits, bucketHash))
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeUnavailableError(ctx, clientRequestId);
            return;
          }
          if (res.status() == TokenProtocol.ST_COLLISION) writeCollisionError(ctx, clientRequestId);
          else if (res.status() == TokenProtocol.ST_NOT_FOUND) writeNotFoundError(ctx, clientRequestId);
          else writeAcquireResp(ctx, res.status(), clientRequestId, res.remaining(), res.retryAfterMs());
        }));
  }


  protected void writeResp(ChannelHandlerContext ctx, byte status, int clientRequestId, long remaining, long retryAfter) {
    writeAcquireResp(ctx, status, clientRequestId, remaining, retryAfter);
  }

  protected void writeNotFoundError(ChannelHandlerContext ctx, int clientRequestId) {
    writeAcquireResp(ctx, ApiProtocol.ST_NOT_FOUND, clientRequestId, REMAINING_ERROR, RETRY_AFTER_MS_NOT_FOUND);
  }

  protected void writeUnavailableError(ChannelHandlerContext ctx, int clientRequestId) {
    writeAcquireResp(ctx, ApiProtocol.ST_UNAVAILABLE, clientRequestId, REMAINING_ERROR, RETRY_AFTER_MS_ERROR);
  }

  protected void writeCollisionError(ChannelHandlerContext ctx, int clientRequestId) {
    writeAcquireResp(ctx, ApiProtocol.ST_COLLISION, clientRequestId, REMAINING_ERROR, RETRY_AFTER_MS_ERROR);
  }

  protected void writeAcquireResp(ChannelHandlerContext ctx, byte status, int requestId, long remaining, long retryAfterMs) {
    PrometheusMetrics metrics = MetricsHolder.get();
    if (metrics != null) metrics.recordAcquire(status);
    ApiResponseCodec.writeAcquire(ctx, status, requestId, remaining, retryAfterMs);
  }
}
