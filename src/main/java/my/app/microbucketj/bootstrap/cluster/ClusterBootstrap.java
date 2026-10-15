package my.app.microbucketj.bootstrap.cluster;

import org.slf4j.Logger;
import org.slf4j.MDC;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.LeaderLocator;
import my.app.microbucketj.cluster.LeadershipRuntime;
import my.app.microbucketj.cluster.discovery.SeedlessDiscovery;
import my.app.microbucketj.cluster.model.HostAndPort;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.Config;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class ClusterBootstrap {

  private ClusterBootstrap() {
  }

  public static ClusterComponents bootstrap(Logger log, Config cfg) {
    SeedlessDiscovery discovery = new SeedlessDiscovery(cfg.discoveryDns, cfg.discoveryHosts, cfg.clusterPort);

    Node self = cfg.selfNode();
    MembershipRuntime membership = new MembershipRuntime(cfg.nodeId, cfg.replicaCount, 1, List.of(self));

    TokenPeerClient tokenPeers = new TokenPeerClient(
        cfg.ioThreads,
        cfg.internalMaxFrame,
        cfg.heartbeatIntervalMs,
        cfg.heartbeatTimeoutMs,
        cfg.nodeId,
        cfg.reconnectPeriodMs,
        cfg.timeouts.forwardTimeoutMs()
    );

    ClusterPeerClient clusterPeers = new ClusterPeerClient(
        cfg.ioThreads,
        cfg.internalMaxFrame,
        cfg.heartbeatIntervalMs,
        cfg.heartbeatTimeoutMs,
        cfg.nodeId,
        cfg.reconnectPeriodMs
    );

    LeadershipRuntime leadershipRuntime = new LeadershipRuntime(cfg.nodeId, cfg.leaderHoldDownMs);

    var snap = membership.snapshot();
    tokenPeers.connectAll(snap.nodes(), snap.view().self());
    clusterPeers.connectAll(snap.nodes(), snap.view().self());

    ScheduledExecutorService tick = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "cluster-tick");
      t.setDaemon(true);
      t.setUncaughtExceptionHandler((th, ex) -> log.error("uncaught in {}", th.getName(), ex));
      return t;
    });

    tick.scheduleWithFixedDelay(() -> {
      MDC.put("nodeId", cfg.nodeId);
      try {
        var current = membership.snapshot();
        Node me = current.view().self();

        tokenPeers.connectAll(current.nodes(), me);
        clusterPeers.connectAll(current.nodes(), me);

        boolean changed = leadershipRuntime.recompute(
            current,
            node -> clusterPeers.isConnected(node) || tokenPeers.isConnected(node)
        );
        if (changed) {
          log.info("[leader] elected={}", leadershipRuntime.leaderId());
        }

        if (current.nodes().size() <= 1) {
          for (HostAndPort result : discovery.candidates(cfg.advertiseHost, cfg.clusterPort)) {
            clusterPeers.sendJoin(result.host(), result.port(), me);
          }
        }
      } catch (Exception e) {
        log.debug("tick failed (best-effort)", e);
      }
    }, cfg.reconnectPeriodMs, cfg.reconnectPeriodMs, TimeUnit.MILLISECONDS);

    return new ClusterComponents(discovery, membership, leadershipRuntime, tokenPeers, clusterPeers, tick);
  }

  public record ClusterComponents(
      SeedlessDiscovery discovery,
      MembershipRuntime membership,
      LeaderLocator leadership,
      TokenPeerClient tokenPeers,
      ClusterPeerClient clusterPeers,
      ScheduledExecutorService tick
  ) {
  }
}
