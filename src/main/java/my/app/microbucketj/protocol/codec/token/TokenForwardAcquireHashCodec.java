package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.TokenProtocol;

public final class TokenForwardAcquireHashCodec {

  private TokenForwardAcquireHashCodec() {
  }

  public static void writeRequest(ByteBuf out, int requestId, long permits, long bucketHash) {
    out.writeByte(TokenProtocol.OP_FWD_ACQUIRE_HASH);
    out.writeInt(requestId);
    out.writeLong(permits <= 0 ? 1 : permits);
    out.writeLong(bucketHash);
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static long readPermits(ByteBuf in) {
    return in.readLong();
  }

  public static long readBucketHash(ByteBuf in) {
    return in.readLong();
  }
}
