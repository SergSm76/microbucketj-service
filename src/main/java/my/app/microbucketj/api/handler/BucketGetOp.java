package my.app.microbucketj.api.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiBucketAdminCodec;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_BUCKET_REQ;

public record BucketGetOp(BucketRegistry buckets) implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BucketGetOp.class);

  public BucketGetOp(BucketRegistry buckets) {
    this.buckets = Objects.requireNonNull(buckets, "buckets");
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_BUCKET_GET;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_BUCKET_REQ) return;
    int requestId = ApiBucketAdminCodec.readRequestId(in);

    String bucket = ApiBucketAdminCodec.readBucketName(in);
    if (bucket == null || bucket.isBlank()) {
      var m = MetricsHolder.get();
      if (m != null) m.recordBucketGet(ApiProtocol.ST_BAD_REQUEST);
      ApiBucketAdminCodec.writeGetResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    BucketSpec cur = buckets.getSpec(bucket);
    if (cur == null) {
      var m = MetricsHolder.get();
      if (m != null) m.recordBucketGet(ApiProtocol.ST_NOT_FOUND);
      ApiBucketAdminCodec.writeGetResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
      LOG.debug("Операция get, бакет не найден: bucket={}", bucket);
      return;
    }

    var m = MetricsHolder.get();
    if (m != null) m.recordBucketGet(ApiProtocol.ST_OK);
    ApiBucketAdminCodec.writeGetResponse(ctx, ApiProtocol.ST_OK, requestId, cur);
  }
}
