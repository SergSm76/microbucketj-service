package my.app.microbucketj.token;


import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.AbstractOpcodeDispatcher;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.handler.PingOp;
import my.app.microbucketj.token.handler.PongOp;
import my.app.microbucketj.token.handler.forward.ForwardAcquireHashOp;
import my.app.microbucketj.token.handler.forward.ForwardAcquireNameOp;
import my.app.microbucketj.token.handler.forward.ForwardReportHashOp;
import my.app.microbucketj.token.handler.forward.ForwardReportNameOp;
import my.app.microbucketj.token.handler.replication.ReplPutBatchOp;
import my.app.microbucketj.token.handler.replication.ReplPutOp;
import my.app.microbucketj.token.handler.replication.StateXferOp;

public final class TokenServerHandler extends AbstractOpcodeDispatcher {

  public TokenServerHandler(MembershipRuntime membership,
                            BucketRegistry buckets,
                            ReplicationService replication) {
    super(
        new PingOp(),
        new PongOp(),
        new ForwardAcquireHashOp(membership, buckets, replication),
        new ForwardAcquireNameOp(membership, buckets, replication),
        new ForwardReportHashOp(membership, buckets, replication),
        new ForwardReportNameOp(membership, buckets, replication),
        new ReplPutOp(buckets),
        new ReplPutBatchOp(buckets),
        new StateXferOp(buckets)
    );
  }
}
