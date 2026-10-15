package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;
import my.app.microbucketj.protocol.codec.common.PingPongCodec;

public final class TokenPingPongCodec {

  private TokenPingPongCodec() {
  }

  public static long readIdOrZero(ByteBuf in) {
    return PingPongCodec.readIdOrZero(in);
  }

  public static void writePayload(ByteBuf out, byte opcode, long id) {
    PingPongCodec.writePayload(out, opcode, id);
  }

  public static void writeFramedPong(ChannelHandlerContext ctx, long id) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, TokenProtocol.PING_PONG_PAYLOAD_LEN);
    out.writeByte(TokenProtocol.OP_PONG);
    out.writeLong(id);
    ctx.writeAndFlush(out);
  }
}
