package my.app.microbucketj.api.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiBucketAdminCodec;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_BUCKET_REQ;


public final class BucketUpsertOp implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BucketUpsertOp.class);

  private final MembershipRuntime membership;
  private final BucketRegistry buckets;
  private final ClusterPeerClient clusterPeers;

  public BucketUpsertOp(MembershipRuntime membership,
                        BucketRegistry buckets,
                        ClusterPeerClient clusterPeers) {
    this.membership = Objects.requireNonNull(membership, "membership");
    this.buckets = Objects.requireNonNull(buckets, "buckets");
    this.clusterPeers = Objects.requireNonNull(clusterPeers, "clusterPeers");
  }

  private static void writeUpsert(ChannelHandlerContext ctx, byte status, int requestId, boolean applied, BucketSpec current) {
    var metrics = MetricsHolder.get();
    if (metrics != null) metrics.recordBucketUpsert(status);
    ApiBucketAdminCodec.writeUpsertResponse(ctx, status, requestId, applied, current);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_BUCKET_UPSERT;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_BUCKET_REQ + 2 + 8) return;
    int requestId = ApiBucketAdminCodec.readRequestId(in);

    String bucket = ApiBucketAdminCodec.readBucketName(in);
    if (bucket == null || bucket.isBlank()) {
      writeUpsert(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, false, null);
      return;
    }

    final BucketSpec spec;
    try {
      spec = ApiBucketAdminCodec.readUpsertSpec(bucket, in);
    } catch (Exception e) {
      writeUpsert(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, false, null);
      return;
    }
    if (spec == null) {
      writeUpsert(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, false, null);
      return;
    }

    BucketRegistry.UpsertResult res = buckets.upsert(spec);
    BucketSpec current = res.current();
    if (current == null) {
      writeUpsert(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, false, null);
      return;
    }

    if (res.applied()) {
      var snap = membership.snapshot();
      clusterPeers.broadcastBucketUpsert(snap.nodes(), snap.view().self(), current);
    }
    logApplied(res.applied(), current);
    writeUpsert(ctx, ApiProtocol.ST_OK, requestId, res.applied(), current);
  }

  private void logApplied(boolean applied, BucketSpec current) {
    if (!applied || current == null) return;
    if (current.isTokenBucket()) {
      LOG.info("Операция upsert, бакет применен: bucket={} type=token ver={} cap={} refill={} periodMs={}",
          current.name(), current.version(), current.capacity(), current.refillTokens(), current.refillPeriodMs());
      return;
    }
    LOG.info("Операция upsert, бакет применен: bucket={} type=adaptive ver={} cap={} refill={} periodMs={} windowMs={} minRequests={} failurePct={} minFactorPct={} downStepPct={} upStepPct={} stepWarmup={}",
        current.name(), current.version(), current.capacity(), current.refillTokens(), current.refillPeriodMs(),
        current.adaptiveWindowMs(), current.adaptiveMinRequests(), current.adaptiveFailureRatePct(),
        current.adaptiveMinFactorPct(), current.adaptiveDecreaseStepPct(), current.adaptiveIncreaseStepPct(), current.stepped());
  }
}