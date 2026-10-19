package my.app.microbucketj.protocol.codec.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

import java.nio.charset.StandardCharsets;

public final class ApiLockCodec {
  private ApiLockCodec() {
  }

  public static int readRequestId(ByteBuf in) {
    return in.readInt();
  }

  public static String readLockName(ByteBuf in) {
    return readLimitedString(in, ApiProtocol.MAX_LOCK_NAME_BYTES);
  }

  public static String readLockedBy(ByteBuf in) {
    return readLimitedString(in, ApiProtocol.MAX_LOCK_OWNER_BYTES);
  }

  public static long readLockUntil(ByteBuf in) {
    return in.readLong();
  }

  private static String readLimitedString(ByteBuf in, int maxBytes) {
    if (in.readableBytes() < 1) return null;
    int len = in.readUnsignedByte();
    if (len <= 0 || len > maxBytes) return null;
    if (in.readableBytes() < len) return null;
    byte[] b = new byte[len];
    in.readBytes(b);
    return new String(b, StandardCharsets.UTF_8);
  }

  /** Ответ для acquire/update/release: status + requestId + lockUntil + lockedAt. */
  public static void writeLockResponse(ChannelHandlerContext ctx, byte status, int requestId, LockEntry e) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_LOCK_SIZE);
    out.writeByte(status);
    out.writeInt(requestId);
    out.writeLong(e == null ? 0L : e.lockUntilMs());
    out.writeLong(e == null ? 0L : e.lockedAtMs());
    ctx.writeAndFlush(out);
  }

  /** Ответ для get: + lockedBy. */
  public static void writeLockGetResponse(ChannelHandlerContext ctx, byte status, int requestId, LockEntry e) {
    byte[] owner = (e == null) ? new byte[0] : e.lockedBy().getBytes(StandardCharsets.UTF_8);
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_LOCK_SIZE + 1 + owner.length);
    out.writeByte(status);
    out.writeInt(requestId);
    out.writeLong(e == null ? 0L : e.lockUntilMs());
    out.writeLong(e == null ? 0L : e.lockedAtMs());
    out.writeByte(owner.length);
    out.writeBytes(owner);
    ctx.writeAndFlush(out);
  }

  /** Простой ответ (delete): status + requestId. */
  public static void writeSimpleResponse(ChannelHandlerContext ctx, byte status, int requestId) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, ApiProtocol.RESP_LOCK_SIMPLE);
    out.writeByte(status);
    out.writeInt(requestId);
    ctx.writeAndFlush(out);
  }
}