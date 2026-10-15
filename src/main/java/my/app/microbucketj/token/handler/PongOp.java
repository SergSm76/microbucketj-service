package my.app.microbucketj.token.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;

public class PongOp implements OpcodeHandler {

  @Override
  public byte opcode() {
    return TokenProtocol.OP_PONG;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf payload) { /* ignore */ }
}
