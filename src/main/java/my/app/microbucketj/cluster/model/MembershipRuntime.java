package my.app.microbucketj.cluster.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class MembershipRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(MembershipRuntime.class);
  private final String selfId;
  private final int replicaCount;
  private final AtomicReference<Snapshot> ref = new AtomicReference<>();
  public MembershipRuntime(String selfId, int replicaCount, int initialEpoch, List<Node> nodes) {
    this.selfId = selfId;
    this.replicaCount = replicaCount;
    applyMembership(initialEpoch, nodes);
  }

  public Snapshot snapshot() {
    return ref.get();
  }

  public void applyMembership(int epoch, List<Node> nodes) {
    while (true) {
      Snapshot prev = ref.get();
      int prevEpoch = prev == null ? -1 : prev.epoch();
      int prevSize = prev == null ? 0 : prev.nodes().size();

      if (prev != null && epoch <= prevEpoch) {
        return;
      }

      ClusterView view = new ClusterView(selfId, nodes, replicaCount);
      Snapshot next = new Snapshot(epoch, view, List.copyOf(nodes));

      if (ref.compareAndSet(prev, next)) {
        LOG.info("Членство применено: epoch {} -> {} nodes {} -> {}", prevEpoch, epoch, prevSize, next.nodes().size());
        return;
      }
    }
  }

  public record Snapshot(int epoch, ClusterView view, List<Node> nodes) {
  }
}
