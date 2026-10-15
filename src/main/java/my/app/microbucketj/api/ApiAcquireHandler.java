package my.app.microbucketj.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.handler.BucketGetOp;
import my.app.microbucketj.api.handler.BucketUpsertOp;
import my.app.microbucketj.api.handler.acquire.AcquireHashOp;
import my.app.microbucketj.api.handler.acquire.AcquireNameOp;
import my.app.microbucketj.api.handler.easy.Acquire1EasyHashOp;
import my.app.microbucketj.api.handler.easy.Acquire1EasyNameOp;
import my.app.microbucketj.api.handler.easy.AcquireEasyHashOp;
import my.app.microbucketj.api.handler.easy.AcquireEasyNameOp;
import my.app.microbucketj.api.handler.report.ReportHashOp;
import my.app.microbucketj.api.handler.report.ReportNameOp;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.AbstractOpcodeDispatcher;
import my.app.microbucketj.protocol.codec.api.ApiResponseCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

public final class ApiAcquireHandler extends AbstractOpcodeDispatcher {

  public ApiAcquireHandler(java.util.function.BooleanSupplier isLeader,
                           MembershipRuntime membership,
                           BucketRegistry buckets,
                           TokenPeerClient tokenPeers,
                           ClusterPeerClient clusterPeers,
                           ReplicationService replication) {
    super(
        new AcquireHashOp(isLeader, membership, buckets, tokenPeers, replication),
        new AcquireNameOp(isLeader, membership, buckets, tokenPeers, replication),
        new AcquireEasyHashOp(isLeader, membership, buckets, tokenPeers, replication),
        new AcquireEasyNameOp(isLeader, membership, buckets, tokenPeers, replication),
        new Acquire1EasyHashOp(isLeader, membership, buckets, tokenPeers, replication),
        new Acquire1EasyNameOp(isLeader, membership, buckets, tokenPeers, replication),
        new ReportHashOp(isLeader, membership, buckets, tokenPeers, replication),
        new ReportNameOp(isLeader, membership, buckets, tokenPeers, replication),
        new BucketUpsertOp(membership, buckets, clusterPeers),
        new BucketGetOp(buckets)
    );
  }

  @Override
  protected void onUnknownOpcode(ChannelHandlerContext ctx, byte op, ByteBuf payload) {
    if (payload != null && payload.readableBytes() >= 4) {
      int requestId = payload.getInt(payload.readerIndex());
      ApiResponseCodec.writeAcquire(ctx, (byte) 3, requestId, 0, 10);
    }
  }
}
