package my.app.microbucketj.token.handler.forward;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardReportNameCodec;
import my.app.microbucketj.replication.ReplicationService;

public final class ForwardReportNameOp extends AbstractForwardReportCommon implements OpcodeHandler {

  public ForwardReportNameOp(MembershipRuntime membership,
                             BucketRegistry buckets,
                             ReplicationService replication) {
    super(membership, buckets, replication);
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_FWD_REPORT_NAME;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    int requestId = TokenForwardReportNameCodec.readRequestId(in);
    boolean success = TokenForwardReportNameCodec.readSuccess(in);
    String bucket = TokenForwardReportNameCodec.readBucket(in);
    AbstractBucketRuntime rtLocal = buckets.get(bucket);
    if (rtLocal == null) {
      writeFwdResp(ctx, TokenProtocol.ST_NOT_FOUND, requestId, 0, 0);
      return;
    }
    long bucketHash = rtLocal.bucketKey();
    run(ctx, requestId, success, bucketHash, rtLocal);
  }
}
