package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.common.BucketSpecWireCodec;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

public final class ApiBucketAdminCodec {
  private ApiBucketAdminCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static String readBucketName(ByteBuf in) {
    return ApiBucketNameCodec.readBucketName(in);
  }

  public static BucketSpec readUpsertSpec(String bucket, ByteBuf in) {
    return BucketSpecWireCodec.readSpecWithoutName(bucket, in);
  }

  public static void writeGetResponse(ChannelHandlerContext ctx, byte status, int requestId, BucketSpec spec) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_GET_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    BucketSpecWireCodec.writeFixedAdminSpec(out, spec);
    ctx.writeAndFlush(out);
  }

  public static void writeUpsertResponse(ChannelHandlerContext ctx, byte status, int requestId, boolean applied, BucketSpec spec) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_UPSERT_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    out.writeByte(applied ? 1 : 0);
    BucketSpecWireCodec.writeFixedAdminSpec(out, spec);
    ctx.writeAndFlush(out);
  }
}
