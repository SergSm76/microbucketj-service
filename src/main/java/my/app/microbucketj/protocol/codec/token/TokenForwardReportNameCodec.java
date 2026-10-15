package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.common.WireStrings;

public final class TokenForwardReportNameCodec {

  private TokenForwardReportNameCodec() {
  }

  public static void writeRequest(ByteBuf out, int requestId, boolean success, String bucket) {
    out.writeByte(TokenProtocol.OP_FWD_REPORT_NAME);
    out.writeInt(requestId);
    out.writeByte(success ? 1 : 0);
    WireStrings.writeU8(out, bucket);
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static boolean readSuccess(ByteBuf in) {
    return in.readByte() == 1;
  }

  public static String readBucket(ByteBuf in) {
    return WireStrings.readU8(in);
  }
}
