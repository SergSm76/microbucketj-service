package my.app.microbucketj.api.handler.report;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiResponseCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.Objects;
import java.util.function.BooleanSupplier;

public abstract class AbstractReportCommon implements OpcodeHandler {

  private final BooleanSupplier isLeader;
  private final MembershipRuntime membership;
  private final BucketRegistry buckets;
  private final TokenPeerClient tokenPeers;
  private final ReplicationService replication;

  protected AbstractReportCommon(BooleanSupplier isLeader,
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

  protected void runReport(ChannelHandlerContext ctx, int requestId, boolean success, long bucketHash, String bucketName) {
    if (bucketHash == Long.MIN_VALUE) {
      writeReportResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
      return;
    }

    AbstractBucketRuntime local = (bucketName != null) ? buckets.get(bucketName) : buckets.getByHashUnique(bucketHash);
    if (local == null) {
      if (bucketName == null && buckets.isCollisionHash(bucketHash)) writeReportResp(ctx, ApiProtocol.ST_COLLISION, requestId);
      else writeReportResp(ctx, ApiProtocol.ST_NOT_FOUND, requestId);
      return;
    }
    if (local.spec().type() == BucketType.TOKEN) {
      writeReportResp(ctx, ApiProtocol.ST_OK, requestId);
      return;
    }

    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      writeReportResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(bucketHash);

    if (primary.id().equals(view.self().id())) {
      AbstractBucketRuntime.ReportResult rr;
      try {
        rr = local.reportAsPrimary(success);
      } catch (Exception e) {
        writeReportResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
        return;
      }

      if (!rr.mutated()) {
        writeReportResp(ctx, rr.status(), requestId);
        return;
      }

      var replF = replication.replicatePrimaryMutation(view, snap.epoch(), bucketHash, local);
      if (replF.isDone()) {
        boolean ok = replF.getNow(false);
        writeReportResp(ctx, ok ? rr.status() : ApiProtocol.ST_UNAVAILABLE, requestId);
        return;
      }

      replF.whenComplete((ok, err) -> ctx.executor().execute(() ->
          writeReportResp(ctx, (err == null && ok) ? rr.status() : ApiProtocol.ST_UNAVAILABLE, requestId)));
      return;
    }

    (bucketName != null
        ? tokenPeers.forwardReport(primary, success, bucketName)
        : tokenPeers.forwardReportByHash(primary, success, bucketHash))
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) writeReportResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
          else writeReportResp(ctx, res.status(), requestId);
        }));
  }

  protected void writeReportResp(ChannelHandlerContext ctx, byte status, int requestId) {
    var metrics = MetricsHolder.get();
    if (metrics != null) metrics.recordReport(status);
    ApiResponseCodec.writeReport(ctx, status, requestId);
  }
}
