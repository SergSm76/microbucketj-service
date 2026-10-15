package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.common.WireStrings;

public final class TokenForwardAcquireNameCodec {

  private TokenForwardAcquireNameCodec() {
  }

  public static void writeRequest(ByteBuf out, int requestId, long permits, String bucket) {
    out.writeByte(TokenProtocol.OP_FWD_ACQUIRE_NAME);
    out.writeInt(requestId);
    out.writeLong(permits <= 0 ? 1 : permits);
    WireStrings.writeU8(out, bucket == null ? "" : bucket);
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static long readPermits(ByteBuf in) {
    return in.readLong();
  }

  public static String readBucket(ByteBuf in) {
    return WireStrings.readU8(in);
  }
}
