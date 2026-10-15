package my.app.microbucketj.protocol.codec.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public final class FramedResponseWriter {

  private FramedResponseWriter() {
  }

  public static ByteBuf allocate(ChannelHandlerContext ctx, int payloadSize) {
    ByteBuf out = ctx.alloc().ioBuffer(4 + payloadSize);
    out.writeInt(payloadSize);
    return out;
  }
}
