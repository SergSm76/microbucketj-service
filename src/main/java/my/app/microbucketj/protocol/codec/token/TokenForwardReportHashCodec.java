package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.TokenProtocol;

public final class TokenForwardReportHashCodec {

  private TokenForwardReportHashCodec() {
  }

  public static void writeRequest(ByteBuf out, int requestId, boolean success, long bucketHash) {
    out.writeByte(TokenProtocol.OP_FWD_REPORT_HASH);
    out.writeInt(requestId);
    out.writeByte(success ? 1 : 0);
    out.writeLong(bucketHash);
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
