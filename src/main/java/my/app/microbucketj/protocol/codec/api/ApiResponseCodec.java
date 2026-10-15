package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

public final class ApiResponseCodec {

  private ApiResponseCodec() {
  }

  public static void writeAcquire(ChannelHandlerContext ctx, byte status, int requestId, long remaining, long retryAfterMs) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_ACQUIRE_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    out.writeLong(remaining);
    out.writeLong(retryAfterMs);
    ctx.writeAndFlush(out);
  }

  public static void writeEasy(ChannelHandlerContext ctx, byte status, int requestId) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_EASY_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    ctx.writeAndFlush(out);
  }

  public static void writeReport(ChannelHandlerContext ctx, byte status, int requestId) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_REPORT_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    ctx.writeAndFlush(out);
  }
}
