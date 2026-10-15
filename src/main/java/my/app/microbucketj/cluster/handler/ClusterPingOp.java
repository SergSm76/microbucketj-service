package my.app.microbucketj.cluster.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.cluster.ClusterPingPongCodec;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

public final class ClusterPingOp implements OpcodeHandler {

  @Override
  public byte opcode() {
    return ClusterProtocol.OP_PING;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    long id = ClusterPingPongCodec.readIdOrZero(in);
    ByteBuf out = FramedResponseWriter.allocate(ctx, 1 + 8);
    ClusterPingPongCodec.writePayload(out, ClusterProtocol.OP_PONG, id);
    ctx.writeAndFlush(out);
  }
}
