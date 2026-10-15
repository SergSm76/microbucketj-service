package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.NameAndHash;

public final class ApiReportNameCodec {

  private ApiReportNameCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static boolean readSuccess(ByteBuf in) {
    return in.readByte() == 1;
  }

  public static NameAndHash readBucket(ByteBuf in) {
    return ApiBucketNameCodec.readNameAndHash(in);
  }
}
