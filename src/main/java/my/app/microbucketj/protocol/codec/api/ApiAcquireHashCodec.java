package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;

public final class ApiAcquireHashCodec {
  private ApiAcquireHashCodec() {
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
