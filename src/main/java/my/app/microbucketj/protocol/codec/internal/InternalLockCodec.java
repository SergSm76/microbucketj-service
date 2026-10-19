package my.app.microbucketj.protocol.codec.internal;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Codec для forward lock-операций между узлами кластера.
 */
public final class InternalLockCodec {
  private InternalLockCodec() {
  }

  public static void writeLockAcquirePayload(ByteBuf out, String lockName, long lockUntilMs, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_ACQUIRE);
    writeLockRequest(out, lockName, lockUntilMs, lockedBy);
  }

  public static void writeLockReleasePayload(ByteBuf out, String lockName, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_RELEASE);
    writeLockRequest(out, lockName, 0L, lockedBy);
  }

  public static void writeLockUpdatePayload(ByteBuf out, String lockName, long lockUntilMs, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_UPDATE);
    writeLockRequest(out, lockName, lockUntilMs, lockedBy);
  }

  public static void writeLockDeletePayload(ByteBuf out, String lockName) {
    out.writeByte(InternalProtocol.OP_LOCK_DELETE);
    byte[] name = lockName.getBytes(UTF_8);
    out.writeByte(name.length);
    out.writeBytes(name);
  }

  private static void writeLockRequest(ByteBuf out, String lockName, long lockUntilMs, String lockedBy) {
    byte[] name = lockName.getBytes(UTF_8);
    out.writeByte(name.length);
    out.writeBytes(name);
    out.writeLong(lockUntilMs);
    if (lockedBy != null) {
      byte[] owner = lockedBy.getBytes(UTF_8);
      out.writeByte(owner.length);
      out.writeBytes(owner);
    } else {
      out.writeByte(0);
    }
  }

  public static LockRequest readLockRequest(ByteBuf in) {
    String name = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    if (name == null || in.readableBytes() < 8) return null;
    long lockUntilMs = in.readLong();
    String lockedBy = readString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
    return new LockRequest(name, lockUntilMs, lockedBy);
  }

  public static String readLockDeleteRequest(ByteBuf in) {
    return readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
  }

  private static String readString(ByteBuf in, int maxBytes) {
    if (in.readableBytes() < 1) return null;
    int len = in.readUnsignedByte();
    if (len <= 0 || len > maxBytes) return null;
    if (in.readableBytes() < len) return null;
    byte[] b = new byte[len];
    in.readBytes(b);
    return new String(b, UTF_8);
  }

  public record LockRequest(String name, long lockUntilMs, String lockedBy) {
  }
}
