package my.app.microbucketj.protocol.codec.common;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public final class WireStrings {

  private WireStrings() {
  }

  public static String readU8(ByteBuf in) {
    if (in.readableBytes() < 1) throw new IllegalArgumentException("not enough bytes for string len");
    int len = in.readUnsignedByte();
    if (in.readableBytes() < len) throw new IllegalArgumentException("not enough bytes for string");
    if (len == 0) return "";
    byte[] bytes = new byte[len];
    in.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static String tryReadU8(ByteBuf in) {
    if (in.readableBytes() < 1) return null;
    int len = in.getUnsignedByte(in.readerIndex());
    if (in.readableBytes() < 1 + len) return null;
    return readU8(in);
  }

  public static void writeU8(ByteBuf out, String value) {
    String s = (value == null) ? "" : value;
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 255) {
      throw new IllegalArgumentException("string utf8 bytes > 255: " + bytes.length);
    }
    out.writeByte(bytes.length);
    out.writeBytes(bytes);
  }
}
