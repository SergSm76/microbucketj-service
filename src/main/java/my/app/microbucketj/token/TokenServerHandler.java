package my.app.microbucketj.token;

import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.AbstractOpcodeDispatcher;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.handler.PingOp;
import my.app.microbucketj.token.handler.PongOp;
import my.app.microbucketj.token.handler.forward.ForwardAcquireHashOp;
import my.app.microbucketj.token.handler.forward.ForwardAcquireNameOp;
import my.app.microbucketj.token.handler.forward.ForwardLockAcquireOp;
import my.app.microbucketj.token.handler.forward.ForwardLockDeleteOp;
import my.app.microbucketj.token.handler.forward.ForwardLockReleaseOp;
import my.app.microbucketj.token.handler.forward.ForwardLockUpdateOp;
import my.app.microbucketj.token.handler.forward.ForwardReportHashOp;
import my.app.microbucketj.token.handler.forward.ForwardReportNameOp;
import my.app.microbucketj.token.handler.replication.ReplLockOp;
import my.app.microbucketj.token.handler.replication.ReplPutBatchOp;
import my.app.microbucketj.token.handler.replication.ReplPutOp;
import my.app.microbucketj.token.handler.replication.StateXferOp;

public final class TokenServerHandler extends AbstractOpcodeDispatcher {

  public TokenServerHandler(MembershipRuntime membership,
                            BucketRegistry buckets,
                            ReplicationService replication,
                            LockRegistry locks) {
    super(
        new PingOp(),
        new PongOp(),
        new ForwardAcquireHashOp(membership, buckets, replication),
        new ForwardAcquireNameOp(membership, buckets, replication),
        new ForwardReportHashOp(membership, buckets, replication),
        new ForwardReportNameOp(membership, buckets, replication),
        new ReplPutOp(buckets),
        new ReplPutBatchOp(buckets),
        new StateXferOp(buckets),
        // НОВЫЕ Lock handlers
        new ForwardLockAcquireOp(membership, locks, replication),
        new ForwardLockReleaseOp(membership, locks, replication),
        new ForwardLockUpdateOp(membership, locks, replication),
        new ForwardLockDeleteOp(membership, locks, replication),
        new ReplLockOp(locks)
    );
  }
}
