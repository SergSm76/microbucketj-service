package my.app.microbucketj.cluster;

import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.handler.ClusterBucketUpsertOp;
import my.app.microbucketj.cluster.handler.ClusterPingOp;
import my.app.microbucketj.cluster.handler.ClusterPongOp;
import my.app.microbucketj.cluster.handler.JoinOp;
import my.app.microbucketj.cluster.handler.MembershipOp;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.dispatch.AbstractOpcodeDispatcher;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

// TODO
public final class ClusterServerHandler extends AbstractOpcodeDispatcher {

  public ClusterServerHandler(LeaderLocator leadership,
                              MembershipRuntime membership,
                              ClusterPeerClient clusterPeers,
                              TokenPeerClient tokenPeers,
                              BucketRegistry buckets,
                              ReplicationService replication) {
    super(new ClusterPingOp(),
        new ClusterPongOp(),
        new JoinOp(leadership::isLeader, membership, clusterPeers, tokenPeers),
        new MembershipOp(membership, clusterPeers, tokenPeers, buckets, replication),
        new ClusterBucketUpsertOp(buckets));
  }
}
