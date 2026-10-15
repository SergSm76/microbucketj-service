package my.app.microbucketj.bootstrap;

import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.LeaderLocator;
import my.app.microbucketj.cluster.discovery.SeedlessDiscovery;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.server.DefaultServer;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record RuntimeComponents(
    SeedlessDiscovery discovery,
    MembershipRuntime membership,
    LeaderLocator leadership,
    TokenPeerClient tokenPeers,
    ClusterPeerClient clusterPeers,
    ReplicationService replication,
    DefaultServer internalServer,
    DefaultServer publicServer,
    ScheduledExecutorService tick
) implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeComponents.class);


  @Override
  public void close() {
    try {
      tick.shutdownNow();
    } catch (Exception e) {
      LOG.warn("Не удалось остановить tick scheduler", e);
    }
    try {
      publicServer.close();
    } catch (Exception e) {
      LOG.warn("Не удалось закрыть public server", e);
    }
    try {
      internalServer.close();
    } catch (Exception e) {
      LOG.warn("Не удалось закрыть internal server", e);
    }
    try {
      clusterPeers.close();
    } catch (Exception e) {
      LOG.warn("Не удалось закрыть cluster peers", e);
    }
    try {
      tokenPeers.close();
    } catch (Exception e) {
      LOG.warn("Не удалось закрыть token peers", e);
    }
    try {
      replication.close();
    } catch (Exception e) {
      LOG.warn("Не удалось закрыть replication service", e);
    }
  }
}
