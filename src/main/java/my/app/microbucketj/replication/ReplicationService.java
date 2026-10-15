package my.app.microbucketj.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.config.ReplicationSettings;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class ReplicationService implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationService.class);

  private static final CompletableFuture<Boolean> CF_TRUE = CompletableFuture.completedFuture(true);
  private static final CompletableFuture<Boolean> CF_FALSE = CompletableFuture.completedFuture(false);

  private final MembershipRuntime membership;
  private final TokenPeerClient tokenPeers;

  private final QuorumMode quorumMode;
  private final int minAcks;
  private final long ackTimeoutMs;
  private final boolean strictAcks;

  private final ReplicationMode mode;
  private final long batchMs;

  private final ScheduledExecutorService scheduler;
  private final ConcurrentHashMap<String, Pending> pending;

  public ReplicationService(MembershipRuntime membership,
                            TokenPeerClient tokenPeers,
                            ReplicationSettings settings) {
    this.membership = Objects.requireNonNull(membership, "membership");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
    ReplicationSettings s = Objects.requireNonNull(settings, "settings");
    this.quorumMode = s.quorumMode() == null ? QuorumMode.CONNECTED_MAJORITY : s.quorumMode();
    this.minAcks = Math.max(0, s.minAcks());
    this.ackTimeoutMs = Math.max(1, s.ackTimeoutMs());
    this.strictAcks = s.strictAcks();
    this.mode = s.mode() == null ? ReplicationMode.BATCH : s.mode();
    this.batchMs = Math.max(1, s.batchMs());

    if (this.mode == ReplicationMode.BATCH) {
      this.pending = new ConcurrentHashMap<>();
      this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "repl-batch");
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((th, ex) -> LOG.error("uncaught in {}", th.getName(), ex));
        return t;
      });
    } else {
      this.pending = null;
      this.scheduler = null;
    }
  }

  private static boolean hasFollowers(ClusterView view, List<Node> replicas) {
    String selfId = view.self().id();
    for (Node node : replicas) {
      if (!selfId.equals(node.id())) return true;
    }
    return false;
  }

  public ReplicationMode mode() {
    return mode;
  }

  public boolean isEnabled() {
    return mode != ReplicationMode.DISABLED;
  }

  public CompletableFuture<Boolean> replicatePrimaryMutation(ClusterView view, int epoch, long bucketHash, AbstractBucketRuntime runtime) {
    if (runtime == null) return CF_TRUE;
    if (mode == ReplicationMode.DISABLED) return CF_TRUE;
    if (view == null) return CF_FALSE;

    Node self = view.self();
    Node primary = view.pickPrimary(bucketHash);
    if (!primary.id().equals(self.id())) return CF_FALSE;

    List<Node> replicas = view.pickReplicas(bucketHash);
    boolean hasFollowers = hasFollowers(view, replicas);
    if (!hasFollowers) return CF_TRUE;

    if (mode == ReplicationMode.BATCH) {
      offerBatch(bucketHash, runtime);
      return CF_TRUE;
    }

    Replication.Plan plan = Replication.plan(view, replicas, tokenPeers, quorumMode, minAcks);
    int need = plan.needAcks();
    if (need == 0 || plan.targets().isEmpty()) return CF_TRUE;

    BucketState state = runtime.snapshot();
    return Replication.replicatePutUntilQuorum(tokenPeers, plan.targets(), epoch, state, need, ackTimeoutMs)
        .handle((acks, err) -> err == null && (!strictAcks || acks >= need));
  }

  private void offerBatch(long bucketHash, AbstractBucketRuntime runtime) {
    String bucket = runtime.spec().name();
    if (bucket == null || bucket.isBlank()) return;
    Pending p = pending.computeIfAbsent(bucket, ignored -> new Pending(bucketHash));
    p.runtime.set(runtime);
    p.latestSeq.set(runtime.seq());
    if (p.scheduled.compareAndSet(false, true)) {
      scheduler.schedule(() -> flush(p), batchMs, TimeUnit.MILLISECONDS);
    }
  }

  private void flush(Pending p) {
    captureSnapshot(p).whenComplete((state, err) -> {
      boolean scheduleAgain = false;
      try {
        if (err != null || state == null) {
          scheduleAgain = p.runtime.get() != null;
          if (err != null) {
            LOG.debug("Не удалось выполнить batch replication snapshot для bucketHash={}", p.bucketHash, err);
          }
          return;
        }

        var snap = membership.snapshot();
        ClusterView view = snap.view();
        Node self = view.self();
        Node primary = view.pickPrimary(p.bucketHash);

        if (!primary.id().equals(self.id())) {
          return;
        }

        List<Node> replicas = view.pickReplicas(p.bucketHash);
        Replication.Plan plan = Replication.plan(view, replicas, tokenPeers, quorumMode, minAcks);
        if (plan.targets().isEmpty()) {
          return;
        }

        int epoch = snap.epoch();
        for (Node node : plan.targets()) {
          tokenPeers.replicatePutBatch(node, epoch, state);
        }

        scheduleAgain = p.latestSeq.get() != state.seq();
      } catch (Exception e) {
        scheduleAgain = true;
        LOG.error("Не удалось выполнить batch replication flush (best-effort)", e);
      } finally {
        p.scheduled.set(false);
      }

      if (scheduleAgain && p.scheduled.compareAndSet(false, true)) {
        scheduler.schedule(() -> flush(p), batchMs, TimeUnit.MILLISECONDS);
      }
    });
  }

  private CompletableFuture<BucketState> captureSnapshot(Pending p) {
    AbstractBucketRuntime runtime = p.runtime.get();
    if (runtime == null) return CompletableFuture.completedFuture(null);
    try {
      return CompletableFuture.completedFuture(runtime.snapshot());
    } catch (Exception e) {
      CompletableFuture<BucketState> fut = new CompletableFuture<>();
      fut.completeExceptionally(e);
      return fut;
    }
  }

  @Override
  public void close() {
    if (scheduler != null) scheduler.shutdownNow();
  }

  private static final class Pending {
    final long bucketHash;
    final AtomicReference<AbstractBucketRuntime> runtime = new AtomicReference<>();
    final AtomicLong latestSeq = new AtomicLong(0);
    final AtomicBoolean scheduled = new AtomicBoolean(false);

    Pending(long bucketHash) {
      this.bucketHash = bucketHash;
    }
  }
}
