package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;

public final class ApiAcquire1HashCodec {
  private ApiAcquire1HashCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static long readBucketHash(ByteBuf in) {
    return in.readLong();
  }
}
