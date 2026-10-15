package my.app.microbucketj.api.handler.easy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.NameAndHash;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiAcquire1NameCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_ACQUIRE_1_EASY_NAME_REQ;
import static my.app.microbucketj.utils.Constants.ONE_PERMIT;

public final class Acquire1EasyNameOp extends AbstractAcquireEasyCommon {
  public Acquire1EasyNameOp(BooleanSupplier isLeader,
                            MembershipRuntime membership,
                            BucketRegistry buckets,
                            TokenPeerClient tokenPeers,
                            ReplicationService replication) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_ACQUIRE_1_EASY_NAME;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_ACQUIRE_1_EASY_NAME_REQ) return;
    int requestId = ApiAcquire1NameCodec.readRequestId(in);
    NameAndHash nameAndHash = ApiAcquire1NameCodec.readBucket(in);
    if (nameAndHash == null) {
      writeUnavailableError(ctx, requestId);
      return;
    }
    runAcquire(ctx, requestId, ONE_PERMIT, nameAndHash.hash(), nameAndHash.name());
  }
}
