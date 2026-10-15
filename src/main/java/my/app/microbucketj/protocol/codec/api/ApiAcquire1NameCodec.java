package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.NameAndHash;

public final class ApiAcquire1NameCodec {
  private ApiAcquire1NameCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static NameAndHash readBucket(ByteBuf in) {
    return ApiBucketNameCodec.readNameAndHash(in);
  }
}
