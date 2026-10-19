package my.app.microbucketj.token.handler.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import java.util.Objects;

public final class ReplLockOp implements OpcodeHandler {

  private final LockRegistry locks;

  public ReplLockOp(LockRegistry locks) { this.locks = Objects.requireNonNull(locks); }

  @Override public byte opcode() { return InternalProtocol.OP_LOCK_REPL; }

  @Override public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    var req = TokenForwardLockCodec.readLockReplicationRequest(in);
    if (req == null) return;

    boolean ok = false;
    try {
      if (req.deleted()) {
        if (req.lockName() != null) locks.removeRemote(req.lockName());
      } else {
        if (req.entry() != null) locks.applyRemote(req.entry());
      }
      ok = true;
    } catch (Exception e) { ok = false; }

    TokenForwardLockCodec.writeFramedReplicationAck(ctx, req.corrId(), ok);
  }
}
