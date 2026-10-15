package my.app.microbucketj.api.handler.easy;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.handler.acquire.AbstractAcquireCommon;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiResponseCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.function.BooleanSupplier;

public abstract class AbstractAcquireEasyCommon extends AbstractAcquireCommon {

  AbstractAcquireEasyCommon(BooleanSupplier isLeader,
                    MembershipRuntime membership,
                    BucketRegistry buckets,
                    TokenPeerClient tokenPeers,
                    ReplicationService replication
  ) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  protected void writeUnavailableError(ChannelHandlerContext ctx, int clientRequestId) {
    writeEasyResp(ctx, ApiProtocol.ST_UNAVAILABLE, clientRequestId);
  }

  @Override
  protected void writeCollisionError(ChannelHandlerContext ctx, int clientRequestId) {
    writeEasyResp(ctx, ApiProtocol.ST_COLLISION, clientRequestId);
  }

  @Override
  protected void writeNotFoundError(ChannelHandlerContext ctx, int clientRequestId) {
    writeEasyResp(ctx, ApiProtocol.ST_NOT_FOUND, clientRequestId);
  }

  @Override
  protected void writeResp(ChannelHandlerContext ctx, byte status, int clientRequestId, long remaining, long retryAfter) {
    writeEasyResp(ctx, status, clientRequestId);
  }

  private void writeEasyResp(ChannelHandlerContext ctx, byte status, int requestId) {
    ApiResponseCodec.writeEasy(ctx, status, requestId);
  }
}
