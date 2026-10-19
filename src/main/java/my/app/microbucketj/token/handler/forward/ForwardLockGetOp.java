package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Forward LOCK_GET — пришёл по internal-каналу, нужно прочитать локально.
 * Не реплицируем, не делаем роутинг — просто читаем и отвечаем.
 */
public final class ForwardLockGetOp implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ForwardLockGetOp.class);

  private final LockExecutor executor;

  public ForwardLockGetOp(LockExecutor executor) {
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_GET_FWD;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenForwardLockCodec.LockForwardRequest req = TokenForwardLockCodec.readLockGetForwardRequest(in);
    if (req == null) return;

    LOG.debug("Получен перенаправленный LOCK_GET_FWD: reqId={} name={}", req.requestId(), req.lockName());

    if (req.lockName() == null || req.lockName().isBlank()) {
      TokenForwardLockCodec.writeLockGetForwardResponse(ctx, req.requestId(),
          ApiProtocol.ST_BAD_REQUEST, null);
      return;
    }

    LockEntry cur = executor.locks().get(req.lockName());
    if (cur == null) {
      TokenForwardLockCodec.writeLockGetForwardResponse(ctx, req.requestId(),
          ApiProtocol.ST_NOT_FOUND, null);
    } else {
      TokenForwardLockCodec.writeLockGetForwardResponse(ctx, req.requestId(),
          ApiProtocol.ST_OK, cur);
    }
  }
}
