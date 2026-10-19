package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Forward LOCK_DELETE.
 */
public final class ForwardLockDeleteOp extends AbstractForwardLockCommon implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ForwardLockDeleteOp.class);

  public ForwardLockDeleteOp(LockExecutor executor) {
    super(executor);
  }

  @Override
  public byte opcode() {
    return InternalProtocol.OP_LOCK_DELETE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenForwardLockCodec.LockDeleteRequest req = TokenForwardLockCodec.readLockDeleteRequest(in);
    if (req == null) return;

    LOG.debug("Получен перенаправленный LOCK_DELETE: reqId={} name={}", req.requestId(), req.lockName());

    executeAndRespond(ctx, req.requestId(), req.lockName(),
        locks -> new LockRegistry.LockResult(locks.delete(req.lockName()), null),
        ApiProtocol.ST_OK,
        true);  // delete удаляет запись
  }
}
