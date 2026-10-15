package my.app.microbucketj.bootstrap;

import org.slf4j.Logger;
import org.slf4j.MDC;
import my.app.microbucketj.bootstrap.bucket.BucketBootstrap;
import my.app.microbucketj.bootstrap.cluster.ClusterBootstrap;
import my.app.microbucketj.bootstrap.network.NetworkBootstrap;
import my.app.microbucketj.config.Config;

/**
 * All wiring in one place.
 */
public final class MicroBucketBootstrap {

  private MicroBucketBootstrap() {
  }

  public static RuntimeComponents bootstrap(Logger log, Config cfg) throws Exception {
    MDC.put("nodeId", cfg.nodeId);

    // cluster layer
    ClusterBootstrap.ClusterComponents cluster = ClusterBootstrap.bootstrap(log, cfg);

    // bucket/replication layer
    BucketBootstrap.BucketComponents bucket = BucketBootstrap.bootstrap(log, cfg, cluster.membership(), cluster.tokenPeers());

    // network layer
    NetworkBootstrap.NetworkComponents net = NetworkBootstrap.bootstrap(
        cfg,
        cluster.membership(),
        cluster.leadership(),
        cluster.tokenPeers(),
        cluster.clusterPeers(),
        cfg.bucketRegistry,
        bucket.replication()
    );

    return new RuntimeComponents(
        cluster.discovery(),
        cluster.membership(),
        cluster.leadership(),
        cluster.tokenPeers(),
        cluster.clusterPeers(),
        bucket.replication(),
        net.internalServer(),
        net.publicServer(),
        cluster.tick()
    );
  }
}
