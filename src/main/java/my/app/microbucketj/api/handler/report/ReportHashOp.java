package my.app.microbucketj.api.handler.report;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiReportHashCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_REPORT_HASH_REQ;

public final class ReportHashOp extends AbstractReportCommon {

  public ReportHashOp(java.util.function.BooleanSupplier isLeader,
                      MembershipRuntime membership,
                      BucketRegistry buckets,
                      TokenPeerClient tokenPeers,
                      ReplicationService replication) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_REPORT_HASH;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_REPORT_HASH_REQ) return;
    int requestId = ApiReportHashCodec.readRequestId(in);
    boolean success = ApiReportHashCodec.readSuccess(in);
    long bucketHash = ApiReportHashCodec.readBucketHash(in);
    runReport(ctx, requestId, success, bucketHash, null);
  }
}
