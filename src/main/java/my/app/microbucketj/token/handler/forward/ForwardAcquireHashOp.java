package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireHashCodec;
import my.app.microbucketj.replication.ReplicationService;

public final class ForwardAcquireHashOp extends AbstractForwardAcquireCommon implements OpcodeHandler {

  public ForwardAcquireHashOp(
      MembershipRuntime membership,
      BucketRegistry buckets,
      ReplicationService replication
  ) {
    super(membership, buckets, replication);
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_FWD_ACQUIRE_HASH;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    int requestId = TokenForwardAcquireHashCodec.readRequestId(in);
    long permits = TokenForwardAcquireHashCodec.readPermits(in);
    long bucketHash = TokenForwardAcquireHashCodec.readBucketHash(in);
    if (bucketHash == Long.MIN_VALUE) {
      writeFwdResp(ctx, TokenProtocol.ST_NOT_FOUND, requestId, 0, 0);
      return;
    }
    AbstractBucketRuntime rtLocal = buckets.getByHashUnique(bucketHash);
    if (rtLocal == null) {
      byte st = buckets.isCollisionHash(bucketHash) ? TokenProtocol.ST_COLLISION : TokenProtocol.ST_NOT_FOUND;
      writeFwdResp(ctx, st, requestId, 0, 0);
      return;
    }
    run(ctx, requestId, permits, bucketHash, rtLocal);
  }
}
