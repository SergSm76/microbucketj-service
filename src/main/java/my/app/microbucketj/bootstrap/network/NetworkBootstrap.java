package my.app.microbucketj.bootstrap.network;

import io.netty.handler.ssl.SslContext;
import my.app.microbucketj.api.ApiAcquireHandler;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.ClusterServerHandler;
import my.app.microbucketj.cluster.LeaderLocator;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.config.Config;
import my.app.microbucketj.internal.UnifiedInternalServerHandler;
import my.app.microbucketj.observability.ControlHttpServer;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.server.DefaultServer;
import my.app.microbucketj.token.TokenPeerClient;
import my.app.microbucketj.token.TokenServerHandler;
import my.app.microbucketj.utils.TlsSupport;

public final class NetworkBootstrap {

  private NetworkBootstrap() {
  }

  public static NetworkComponents bootstrap(Config cfg,
                                            MembershipRuntime membership,
                                            LeaderLocator leadership,
                                            TokenPeerClient tokenPeers,
                                            ClusterPeerClient clusterPeers,
                                            BucketRegistry bucketRegistry,
                                            ReplicationService replication) throws Exception {

    DefaultServer internalServer = new DefaultServer("internal",
        cfg.bindHost, cfg.clusterPort, cfg.ioThreads, cfg.internalMaxFrame,
        null,
        () -> new UnifiedInternalServerHandler(
            () -> new ClusterServerHandler(leadership, membership, clusterPeers, tokenPeers, bucketRegistry, replication),
            () -> new TokenServerHandler(membership, bucketRegistry, replication)
        )
    );
    PrometheusMetrics metrics = new PrometheusMetrics(
        cfg.nodeId,
        () -> bucketRegistry.bucketNames().size(),
        () -> {
          var snap = membership.snapshot();
          return snap == null || snap.nodes() == null ? 0 : snap.nodes().size();
        },
        leadership::isLeader
    );
    MetricsHolder.set(metrics);

    SslContext sslContext = null;
    if (cfg.sslConfig.tlsEnabled()) {
      sslContext = TlsSupport.buildServerMtlsContext(cfg.sslConfig);
    }

    DefaultServer publicServer = new DefaultServer("public",
        cfg.bindHost, cfg.publicPort, cfg.ioThreads, cfg.publicMaxFrame,
        sslContext,
        () -> new ApiAcquireHandler(leadership::isLeader, membership, bucketRegistry, tokenPeers, clusterPeers,
            replication)
    );
    ControlHttpServer observabilityServer = null;
    if (cfg.observabilityEnabled) {
      try {
        observabilityServer = new ControlHttpServer(cfg.bindHost, cfg.observabilityPort, metrics);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to start observability server on port " + cfg.observabilityPort, e);
      }
    }


    return new NetworkComponents(internalServer, publicServer, observabilityServer);
  }

  public record NetworkComponents(DefaultServer internalServer, DefaultServer publicServer, ControlHttpServer observabilityServer) {
  }
}
