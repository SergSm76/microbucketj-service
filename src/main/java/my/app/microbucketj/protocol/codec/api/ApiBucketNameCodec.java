package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.NameAndHash;
import my.app.microbucketj.protocol.ApiProtocol;

import java.nio.charset.StandardCharsets;

import static my.app.microbucketj.utils.Hashing.fnv1a64;

public final class ApiBucketNameCodec {
  private ApiBucketNameCodec() {
  }

  public static String readBucketName(ByteBuf in) {
    if (in.readableBytes() < 1) return null;
    int len = in.readUnsignedByte();
    if (len < 0 || len > ApiProtocol.MAX_BUCKET_BYTES) return null;
    if (in.readableBytes() < len) return null;
    byte[] b = new byte[len];
    in.readBytes(b);
    return new String(b, StandardCharsets.UTF_8);
  }

  public static NameAndHash readNameAndHash(ByteBuf in) {
    if (in.readableBytes() < 1) return null;
    int len = in.readUnsignedByte();
    if (len <= 0 || len > ApiProtocol.MAX_BUCKET_BYTES) return null;
    if (in.readableBytes() < len) return null;

    int idx = in.readerIndex();
    long h = fnv1a64(in, idx, len);

    byte[] b = new byte[len];
    in.readBytes(b);
    return new NameAndHash(new String(b, StandardCharsets.UTF_8), h);
  }
}
