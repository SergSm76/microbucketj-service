package my.app.microbucketj.protocol.codec.cluster;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.common.BucketSpecWireCodec;
import my.app.microbucketj.protocol.codec.common.WireStrings;

public final class ClusterBucketUpsertCodec {

  private ClusterBucketUpsertCodec() {
  }

  public static void writePayload(ByteBuf out, BucketSpec spec) {
    out.writeByte(ClusterProtocol.OP_BUCKET_UPSERT);
    WireStrings.writeU8(out, spec.name());
    BucketSpecWireCodec.writeSpecWithoutName(out, spec);
  }

  public static BucketSpec readSpec(ByteBuf in) {
    String bucket = WireStrings.readU8(in);
    if (bucket == null || bucket.isBlank()) return null;
    return BucketSpecWireCodec.readSpecWithoutName(bucket, in);
  }
}
