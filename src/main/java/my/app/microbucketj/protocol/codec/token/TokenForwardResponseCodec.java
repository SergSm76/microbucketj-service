package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

public final class TokenForwardResponseCodec {

  private TokenForwardResponseCodec() {
  }

  public static void writeFramed(ChannelHandlerContext ctx, byte status, int requestId, long remaining, long retryMs) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, TokenProtocol.FWD_RESP_PAYLOAD_LEN);
    out.writeByte(TokenProtocol.OP_FWD_RESP);
    out.writeByte(status);
    out.writeInt(requestId);
    out.writeLong(remaining);
    out.writeLong(retryMs);
    ctx.writeAndFlush(out);
  }

  public static ForwardResponse read(ByteBuf in) {
    byte status = in.readByte();
    int requestId = in.readInt();
    long remaining = in.readLong();
    long retry = in.readLong();
    return new ForwardResponse(status, requestId, remaining, retry);
  }

  public record ForwardResponse(byte status, int requestId, long remaining, long retryAfterMs) {
  }
}
