package my.app.microbucketj.cluster.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ClusterProtocol;

public final class ClusterPongOp implements OpcodeHandler {

  @Override
  public byte opcode() {
    return ClusterProtocol.OP_PONG;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf payload) {
    // ignore
  }
}
