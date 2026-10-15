package my.app.microbucketj.token.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenPingPongCodec;

public final class PingOp implements OpcodeHandler {

  @Override
  public byte opcode() {
    return TokenProtocol.OP_PING;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    long id = TokenPingPongCodec.readIdOrZero(in);
    TokenPingPongCodec.writeFramedPong(ctx, id);
  }
}
