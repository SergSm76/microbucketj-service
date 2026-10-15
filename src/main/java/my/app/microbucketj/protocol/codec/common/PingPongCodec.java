package my.app.microbucketj.protocol.codec.common;

import io.netty.buffer.ByteBuf;

public final class PingPongCodec {

  private PingPongCodec() {
  }

  public static long readIdOrZero(ByteBuf in) {
    return in.readableBytes() >= Long.BYTES ? in.readLong() : 0L;
  }

  public static void writePayload(ByteBuf out, byte opcode, long id) {
    out.writeByte(opcode);
    out.writeLong(id);
  }
}
