package my.app.microbucketj.api.handler.lock;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;

/**
 * Базовый класс для lock-опов. Наследники реализуют opcode() и handle().
 * Здесь — только общие хелперы формирования ответов.
 */
public abstract class AbstractLockCommon implements OpcodeHandler {

  // --- Ответы с LockEntry (acquire/update/release) ---
  protected void writeLockResponse(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry) {
    recordMetric(status);
    ApiLockCodec.writeLockResponse(ctx, status, requestId, entry);
  }

  protected void writeLockGetResponse(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry) {
    recordMetric(status);
    ApiLockCodec.writeLockGetResponse(ctx, status, requestId, entry);
  }

  // --- Простые ответы (delete) ---
  protected void writeSimpleResponse(ChannelHandlerContext ctx, byte status, int requestId) {
    recordMetric(status);
    ApiLockCodec.writeSimpleResponse(ctx, status, requestId);
  }

  // --- Готовые пресеты ---
  protected void writeBadRequest(ChannelHandlerContext ctx, int requestId) {
    writeLockResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
  }

  protected void writeSimpleBadRequest(ChannelHandlerContext ctx, int requestId) {
    writeSimpleResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId);
  }

  protected void writeUnavailable(ChannelHandlerContext ctx, int requestId) {
    writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
  }

  protected void writeSimpleUnavailable(ChannelHandlerContext ctx, int requestId) {
    writeSimpleResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
  }

  protected void writeNotFound(ChannelHandlerContext ctx, int requestId) {
    writeLockResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
  }

  protected void writeSimpleNotFound(ChannelHandlerContext ctx, int requestId) {
    writeSimpleResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId);
  }

  protected void writeSimpleOk(ChannelHandlerContext ctx, int requestId) {
    writeSimpleResponse(ctx, ApiProtocol.ST_OK, requestId);
  }

  protected void writeLockHeld(ChannelHandlerContext ctx, int requestId, LockEntry entry) {
    writeLockResponse(ctx, ApiProtocol.ST_LOCK_HELD, requestId, entry);
  }

  /** Каждый оп реализует запись своей метрики. */
  protected abstract void recordMetric(byte status);
}
