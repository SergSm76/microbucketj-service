package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;

public final class ForwardLockGetOp implements OpcodeHandler {

  private final LockRegistry locks;

  public ForwardLockGetOp(LockRegistry locks) {
    this.locks = locks;
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_GET_FWD;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    var req = TokenForwardLockCodec.readLockGetForwardRequest(in);
    if (req == null) return;
    LockEntry cur = locks.get(req.lockName());
    if (cur == null) {
      TokenForwardLockCodec.writeLockGetForwardResponse(ctx, req.requestId(),
          ApiProtocol.ST_NOT_FOUND, null);
    } else {
      TokenForwardLockCodec.writeLockGetForwardResponse(ctx, req.requestId(),
          ApiProtocol.ST_OK, cur);
    }
  }
}
