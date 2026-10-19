package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import my.app.microbucketj.replication.ReplicationService;

/**
 * Обработчик внутреннего запроса на продление (update) блокировки (пересылка на primary).
 */
public final class ForwardLockUpdateOp extends AbstractForwardLockCommon implements OpcodeHandler {

  public ForwardLockUpdateOp(MembershipRuntime membership,
                             LockRegistry locks,
                             ReplicationService replication) {
    super(membership, locks, replication);
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_UPDATE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenForwardLockCodec.LockForwardRequest req = TokenForwardLockCodec.readLockUpdateRequest(in);
    if (req != null) {
      runUpdate(ctx, req.requestId(), req.lockName(), req.lockUntilMs(), req.lockedBy());
    }
  }
}
