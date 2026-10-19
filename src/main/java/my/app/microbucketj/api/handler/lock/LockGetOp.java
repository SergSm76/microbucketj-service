package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;

/**
 * LockGetOp — read-only операция, выполняется локально без маршрутизации.
 * По аналогии с BucketGetOp.
 */
public record LockGetOp(LockRegistry locks) implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LockGetOp.class);

  public LockGetOp(LockRegistry locks) {
    this.locks = Objects.requireNonNull(locks, "locks");
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_GET;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_GET_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);

    if (name == null || name.isBlank()) {
      var m = MetricsHolder.get();
      if (m != null) m.recordLockGet(ApiProtocol.ST_BAD_REQUEST);
      ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    LockEntry cur = locks.get(name);
    if (cur == null) {
      var m = MetricsHolder.get();
      if (m != null) m.recordLockGet(ApiProtocol.ST_NOT_FOUND);
      ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
      LOG.debug("Операция lock-get, блокировка не найдена: lock={}", name);
      return;
    }

    var m = MetricsHolder.get();
    if (m != null) m.recordLockGet(ApiProtocol.ST_OK);
    ApiLockCodec.writeLockGetResponse(ctx, ApiProtocol.ST_OK, requestId, cur);
  }
}
