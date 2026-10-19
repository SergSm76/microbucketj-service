package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;

/**
 * Forward LOCK_ACQUIRE — пришёл по internal-каналу от другого узла,
 * нужно выполнить на этом узле (он является или должен являться primary).
 */
public final class ForwardLockAcquireOp extends AbstractForwardLockCommon implements OpcodeHandler {

  public ForwardLockAcquireOp(LockExecutor executor) {
    super(executor);
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_ACQUIRE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenForwardLockCodec.LockForwardRequest req = TokenForwardLockCodec.readLockAcquireRequest(in);
    if (req == null) return;

    executeAndRespond(ctx, req.requestId(), req.lockName(),
        locks -> locks.acquire(req.lockName(), req.lockUntilMs(), req.lockedBy()),
        ApiProtocol.ST_OK,
        false);  // acquire записывает
  }
}
