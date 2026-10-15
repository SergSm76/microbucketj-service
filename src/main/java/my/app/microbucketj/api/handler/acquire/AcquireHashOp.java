package my.app.microbucketj.api.handler.acquire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiAcquireHashCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_ACQUIRE_HASH_REQ;

public final class AcquireHashOp extends AbstractAcquireCommon {
  public AcquireHashOp(java.util.function.BooleanSupplier isLeader,
                       MembershipRuntime membership,
                       BucketRegistry buckets,
                       TokenPeerClient tokenPeers,
                       ReplicationService replication) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_ACQUIRE_HASH;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_ACQUIRE_HASH_REQ) return;
    int requestId = ApiAcquireHashCodec.readRequestId(in);
    long permits = ApiAcquireHashCodec.readPermits(in);
    long bucketHash = ApiAcquireHashCodec.readBucketHash(in);
    runAcquire(ctx, requestId, permits, bucketHash);
  }
}
