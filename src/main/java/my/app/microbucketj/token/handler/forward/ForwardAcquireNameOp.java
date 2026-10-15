package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireNameCodec;
import my.app.microbucketj.replication.ReplicationService;

public final class ForwardAcquireNameOp extends AbstractForwardAcquireCommon implements OpcodeHandler {

  public ForwardAcquireNameOp(MembershipRuntime membership, BucketRegistry buckets,
                              ReplicationService replication) {
    super(membership, buckets, replication);
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_FWD_ACQUIRE_NAME;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    int requestId = TokenForwardAcquireNameCodec.readRequestId(in);
    long permits = TokenForwardAcquireNameCodec.readPermits(in);
    String bucket = TokenForwardAcquireNameCodec.readBucket(in);
    if (bucket.isBlank()) {
      writeFwdResp(ctx, TokenProtocol.ST_NOT_FOUND, requestId, 0, 0);
      return;
    }
    AbstractBucketRuntime rtLocal = buckets.get(bucket);
    if (rtLocal == null) {
      writeFwdResp(ctx, TokenProtocol.ST_NOT_FOUND, requestId, 0, 0);
      return;
    }
    run(ctx, requestId, permits, rtLocal.bucketKey(), rtLocal);
  }
}
