package my.app.microbucketj.protocol.codec.cluster;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.codec.common.PingPongCodec;

public final class ClusterPingPongCodec {

  private ClusterPingPongCodec() {
  }

  public static long readIdOrZero(ByteBuf in) {
    return PingPongCodec.readIdOrZero(in);
  }

  public static void writePayload(ByteBuf out, byte opcode, long id) {
    PingPongCodec.writePayload(out, opcode, id);
  }
}
