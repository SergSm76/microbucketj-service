package my.app.microbucketj.api.handler.easy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.NameAndHash;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiAcquireNameCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_ACQUIRE_NAME_REQ;

public final class AcquireEasyNameOp extends AbstractAcquireEasyCommon {
  public AcquireEasyNameOp(BooleanSupplier isLeader,
                           MembershipRuntime membership,
                           BucketRegistry buckets,
                           TokenPeerClient tokenPeers,
                           ReplicationService replication) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_ACQUIRE_EASY_NAME;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_ACQUIRE_NAME_REQ) return;
    int requestId = ApiAcquireNameCodec.readRequestId(in);
    long permits = ApiAcquireNameCodec.readPermits(in);
    NameAndHash nh = ApiAcquireNameCodec.readBucket(in);
    if (nh == null) {
      writeUnavailableError(ctx, requestId);
      return;
    }
    runAcquire(ctx, requestId, permits, nh.hash(), nh.name());
  }
}
