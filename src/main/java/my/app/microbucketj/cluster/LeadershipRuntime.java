package my.app.microbucketj.cluster;

import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public final class LeadershipRuntime implements LeaderLocator {

  private final String selfId;
  private final long holdDownNanos;

  private final AtomicReference<String> leaderId = new AtomicReference<>();
  private final AtomicBoolean leader = new AtomicBoolean(false);

  private volatile String pendingLeaderId;
  private volatile long pendingSinceNanos;

  public LeadershipRuntime(String selfId, long leaderHoldDownMs) {
    this.selfId = Objects.requireNonNullElse(selfId, "");
    long ms = Math.max(0, leaderHoldDownMs);
    this.holdDownNanos = ms == 0 ? 0 : ms * 1_000_000L;

    // Bootstrap: no preselected leader. We will elect on first recompute().
    this.leaderId.set(null);
    this.leader.set(false);
  }

  public boolean isLeader() {
    return leader.get();
  }

  public String leaderId() {
    return leaderId.get();
  }

  public boolean recompute(MembershipRuntime.Snapshot snap, Predicate<Node> alive) {
    if (snap == null) return false;

    String current = leaderId.get();
    String elected = LeaderElection.electLeaderId(snap.nodes(), selfId, alive, current);

    // no change
    if (Objects.equals(current, elected)) {
      pendingLeaderId = null;
      pendingSinceNanos = 0L;
      leader.set(selfId.equals(current));
      return false;
    }

    // First election: apply immediately.
    if (current == null) {
      leaderId.set(elected);
      pendingLeaderId = null;
      pendingSinceNanos = 0L;
      leader.set(selfId.equals(elected));
      return true;
    }

    // Hold-down disabled
    if (holdDownNanos == 0) {
      leaderId.set(elected);
      pendingLeaderId = null;
      pendingSinceNanos = 0L;
      leader.set(selfId.equals(elected));
      return true;
    }

    // Candidate tracking
    long now = System.nanoTime();
    if (!Objects.equals(pendingLeaderId, elected)) {
      pendingLeaderId = elected;
      pendingSinceNanos = now;
      leader.set(selfId.equals(current));
      return false;
    }

    if (now - pendingSinceNanos >= holdDownNanos) {
      leaderId.set(elected);
      pendingLeaderId = null;
      pendingSinceNanos = 0L;
      leader.set(selfId.equals(elected));
      return true;
    }

    leader.set(selfId.equals(current));
    return false;
  }
}
