package my.app.microbucketj.api.handler.report;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.NameAndHash;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiReportNameCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_REPORT_NAME_REQ;

public final class ReportNameOp extends AbstractReportCommon {

  public ReportNameOp(java.util.function.BooleanSupplier isLeader,
                      MembershipRuntime membership,
                      BucketRegistry buckets,
                      TokenPeerClient tokenPeers,
                      ReplicationService replication) {
    super(isLeader, membership, buckets, tokenPeers, replication);
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_REPORT_NAME;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_REPORT_NAME_REQ) return;
    int requestId = ApiReportNameCodec.readRequestId(in);
    boolean success = ApiReportNameCodec.readSuccess(in);
    NameAndHash nh = ApiReportNameCodec.readBucket(in);
    if (nh == null) {
      writeReportResp(ctx, ApiProtocol.ST_NOT_FOUND, requestId);
      return;
    }
    runReport(ctx, requestId, success, nh.hash(), nh.name());
  }
}
