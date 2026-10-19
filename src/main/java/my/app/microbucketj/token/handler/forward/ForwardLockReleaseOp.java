package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;

/**
 * Forward LOCK_RELEASE.
 */
public final class ForwardLockReleaseOp extends AbstractForwardLockCommon implements OpcodeHandler {

  public ForwardLockReleaseOp(LockExecutor executor) {
    super(executor);
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_RELEASE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenForwardLockCodec.LockForwardRequest req = TokenForwardLockCodec.readLockReleaseRequest(in);
    if (req == null) return;

    executeAndRespond(ctx, req.requestId(), req.lockName(),
        locks -> locks.release(req.lockName(), req.lockedBy()),
        ApiProtocol.ST_OK,
        true);  // release удаляет запись
  }
}
