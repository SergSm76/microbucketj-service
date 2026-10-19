package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;

import java.nio.charset.StandardCharsets;

/**
 * Codec для forward lock-операций между узлами кластера.
 */
public final class TokenForwardLockCodec {
  private TokenForwardLockCodec() {
  }

  public static void writeLockAcquireRequest(ByteBuf out, int requestId, String lockName,
                                             long lockUntilMs, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_ACQUIRE);
    out.writeInt(requestId);
    writeString(out, lockName, ApiProtocol.MAX_LOCK_NAME_BYTES);
    out.writeLong(lockUntilMs);
    writeString(out, lockedBy, ApiProtocol.MAX_LOCK_OWNER_BYTES);
  }

  public static void writeLockReleaseRequest(ByteBuf out, int requestId, String lockName, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_RELEASE);
    out.writeInt(requestId);
    writeString(out, lockName, ApiProtocol.MAX_LOCK_NAME_BYTES);
    writeString(out, lockedBy, ApiProtocol.MAX_LOCK_OWNER_BYTES);
  }

  public static void writeLockUpdateRequest(ByteBuf out, int requestId, String lockName,
                                            long lockUntilMs, String lockedBy) {
    out.writeByte(InternalProtocol.OP_LOCK_UPDATE);
    out.writeInt(requestId);
    writeString(out, lockName, ApiProtocol.MAX_LOCK_NAME_BYTES);
    out.writeLong(lockUntilMs);
    writeString(out, lockedBy, ApiProtocol.MAX_LOCK_OWNER_BYTES);
  }

  public static void writeLockDeleteRequest(ByteBuf out, int requestId, String lockName) {
    out.writeByte(InternalProtocol.OP_LOCK_DELETE);
    out.writeInt(requestId);
    writeString(out, lockName, ApiProtocol.MAX_LOCK_NAME_BYTES);
  }

  public static void writeLockReplicationRequest(ByteBuf out, int corrId, int epoch, long lockHash,
                                                 LockEntry entry, boolean deleted) {
    out.writeByte(InternalProtocol.OP_LOCK_REPL);
    out.writeInt(corrId);
    out.writeInt(epoch);
    out.writeLong(lockHash);
    out.writeByte(deleted ? 1 : 0);
    if (!deleted && entry != null) {
      writeEntry(out, entry);
    }
  }

  public static void writeForwardResponse(ByteBuf out, int requestId, byte status, LockEntry entry) {
    out.writeByte(InternalProtocol.OP_LOCK_FWD_RESP);
    out.writeInt(requestId);
    out.writeByte(status);
    if (entry != null) {
      out.writeByte(1);
      writeEntry(out, entry);
    } else {
      out.writeByte(0);
    }
  }

  public static void writeLockReplicationAck(ByteBuf out, int corrId, boolean ok) {
    out.writeByte(InternalProtocol.OP_LOCK_REPL_ACK);
    out.writeInt(corrId);
    out.writeByte(ok ? 1 : 0);
  }

  public static ForwardLockResponse readForwardResponse(ByteBuf in) {
    if (in.readableBytes() < 4 + 1 + 1) return null;
    int requestId = in.readInt();
    byte status = in.readByte();
    byte hasEntry = in.readByte();
    LockEntry entry = null;
    if (hasEntry == 1) {
      entry = readEntry(in);
    }
    return new ForwardLockResponse(requestId, status, entry);
  }

  public static int readCorrId(ByteBuf in) {
    return in.readInt();
  }

  public static boolean readAckOk(ByteBuf in) {
    return in.readByte() == 1;
  }

  public static LockReplicationRequest readLockReplicationRequest(ByteBuf in) {
    if (in.readableBytes() < 4 + 4 + 8 + 1) return null;
    int corrId = in.readInt();
    int epoch = in.readInt();
    long lockHash = in.readLong();
    boolean deleted = in.readByte() == 1;
    LockEntry entry = null;
    if (!deleted) {
      entry = readEntry(in);
    }
    return new LockReplicationRequest(corrId, epoch, lockHash, entry, deleted);
  }

  public static LockForwardRequest readLockAcquireRequest(ByteBuf in) {
    int requestId = in.readInt();
    String lockName = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    if (lockName == null || in.readableBytes() < 8 + 1) return null;
    long lockUntilMs = in.readLong();
    String lockedBy = readString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
    return new LockForwardRequest(requestId, lockName, lockUntilMs, lockedBy);
  }

  public static LockForwardRequest readLockReleaseRequest(ByteBuf in) {
    int requestId = in.readInt();
    String lockName = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    if (lockName == null || in.readableBytes() < 1) return null;
    String lockedBy = readString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
    return new LockForwardRequest(requestId, lockName, 0L, lockedBy);
  }

  public static LockForwardRequest readLockUpdateRequest(ByteBuf in) {
    int requestId = in.readInt();
    String lockName = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    if (lockName == null || in.readableBytes() < 8 + 1) return null;
    long lockUntilMs = in.readLong();
    String lockedBy = readString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
    return new LockForwardRequest(requestId, lockName, lockUntilMs, lockedBy);
  }

  public static LockDeleteRequest readLockDeleteRequest(ByteBuf in) {
    int requestId = in.readInt();
    String lockName = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    return new LockDeleteRequest(requestId, lockName);
  }

  private static void writeString(ByteBuf out, String s, int maxBytes) {
    if (s == null) s = "";
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    if (b.length > maxBytes) {
      byte[] tmp = new byte[maxBytes];
      System.arraycopy(b, 0, tmp, 0, maxBytes);
      b = tmp;
    }
    out.writeByte(b.length);
    out.writeBytes(b);
  }

  private static String readString(ByteBuf in, int maxBytes) {
    if (in.readableBytes() < 1) return null;
    int len = in.readUnsignedByte();
    if (len > maxBytes) return null;
    if (in.readableBytes() < len) return null;
    byte[] b = new byte[len];
    in.readBytes(b);
    return new String(b, StandardCharsets.UTF_8);
  }

  private static void writeEntry(ByteBuf out, LockEntry e) {
    byte[] name = e.name().getBytes(StandardCharsets.UTF_8);
    byte[] owner = e.lockedBy().getBytes(StandardCharsets.UTF_8);
    out.writeByte(name.length);
    out.writeBytes(name);
    out.writeLong(e.lockUntilMs());
    out.writeLong(e.lockedAtMs());
    out.writeByte(owner.length);
    out.writeBytes(owner);
  }

  private static LockEntry readEntry(ByteBuf in) {
    String name = readString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
    if (name == null || in.readableBytes() < 16 + 1) return null;
    long lockUntil = in.readLong();
    long lockedAt = in.readLong();
    String owner = readString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
    if (owner == null) return null;
    try {
      return new LockEntry(name, lockUntil, lockedAt, owner);
    } catch (IllegalArgumentException ex) {
      return null;
    }
  }

  public record ForwardLockResponse(int requestId, byte status, LockEntry entry) {
  }

  public record LockReplicationRequest(int corrId, int epoch, long lockHash, LockEntry entry, boolean deleted) {
  }

  public record LockForwardRequest(int requestId, String lockName, long lockUntilMs, String lockedBy) {
  }

  public record LockDeleteRequest(int requestId, String lockName) {
  }
}
