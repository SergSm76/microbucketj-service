package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;

public final class ApiReportHashCodec {

  private ApiReportHashCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static boolean readSuccess(ByteBuf in) {
    return in.readByte() == 1;
  }

  public static long readBucketHash(ByteBuf in) {
    return in.readLong();
  }
}
