package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.NameAndHash;

public final class ApiAcquireNameCodec {
  private ApiAcquireNameCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static long readPermits(ByteBuf in) {
    return in.readLong();
  }

  public static NameAndHash readBucket(ByteBuf in) {
    return ApiBucketNameCodec.readNameAndHash(in);
  }
}
