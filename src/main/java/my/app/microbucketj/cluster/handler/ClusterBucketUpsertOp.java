package my.app.microbucketj.cluster.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.cluster.ClusterBucketUpsertCodec;

import java.util.Objects;

public record ClusterBucketUpsertOp(BucketRegistry buckets) implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterBucketUpsertOp.class);

  public ClusterBucketUpsertOp(BucketRegistry buckets) {
    this.buckets = Objects.requireNonNull(buckets, "buckets");
  }

  @Override
  public byte opcode() {
    return ClusterProtocol.OP_BUCKET_UPSERT;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    final BucketSpec spec;
    try {
      spec = ClusterBucketUpsertCodec.readSpec(in);
    } catch (Exception e) {
      return;
    }
    if (spec == null) return;

    BucketRegistry.UpsertResult r = buckets.upsert(spec);
    if (r.applied() && r.current() != null) {
      BucketSpec cur = r.current();
      LOG.info("Обновление бакета из кластера применено: bucket={} ver={} type={} adaptive={}",
          cur.name(), cur.version(), cur.type(), cur.isAdaptiveRate());
    }
  }
}
