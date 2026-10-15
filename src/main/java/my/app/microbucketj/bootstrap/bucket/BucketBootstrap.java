package my.app.microbucketj.bootstrap.bucket;

import org.slf4j.Logger;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.config.Config;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

public final class BucketBootstrap {

  private BucketBootstrap() {
  }

  public static BucketComponents bootstrap(Logger log,
                                           Config cfg,
                                           MembershipRuntime membership,
                                           TokenPeerClient tokenPeers) {
    if (cfg.replication.mode() == ReplicationMode.DISABLED) {
      log.warn("replication is disabled: primary mutations stay local and membership handoff/state transfer is off");
    } else if (cfg.replication.mode() == ReplicationMode.BATCH && cfg.replication.strictAcks()) {
      log.warn("STRICT_ACKS=true has no effect with REPLICATION_MODE=BATCH (best-effort)");
    }
    ReplicationService replication = new ReplicationService(membership, tokenPeers, cfg.replication);
    return new BucketComponents(replication);
  }

  public record BucketComponents(ReplicationService replication) {
  }
}
