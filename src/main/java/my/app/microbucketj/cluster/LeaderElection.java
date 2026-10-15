package my.app.microbucketj.cluster;

import my.app.microbucketj.cluster.model.Node;

import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Seedless leader election with failover.
 *
 * Primary use-case: Kubernetes StatefulSet where NODE_ID looks like <name>-<ordinal>.
 *
 * Rule:
 * leader = the smallest available ordinal among "alive" nodes.
 * If ordinal can't be parsed, lexical order by nodeId is used.
 *
 * To reduce flapping, caller can pass currentLeaderId - it will be kept while alive.
 */
public final class LeaderElection {

  private static final Pattern ORDINAL = Pattern.compile("^(.*?)-(\\d+)$");

  private LeaderElection() {
  }

  /**
   * Try to parse StatefulSet ordinal from nodeId.
   */
  public static OptionalLong ordinal(String nodeId) {
    if (nodeId == null) return OptionalLong.empty();
    String id = nodeId.trim();
    if (id.isEmpty()) return OptionalLong.empty();

    Matcher m = ORDINAL.matcher(id);
    if (!m.matches()) return OptionalLong.empty();
    try {
      return OptionalLong.of(Long.parseLong(m.group(2)));
    } catch (NumberFormatException ignored) {
      return OptionalLong.empty();
    }
  }

  /**
   * Elect leaderId from membership nodes.
   *
   * @param nodes           membership nodes (should include self)
   * @param selfId          this nodeId
   * @param alive           predicate for liveness (can be null; self is always treated as alive)
   * @param currentLeaderId if still alive, will be preserved
   */
  public static String electLeaderId(List<Node> nodes,
                                     String selfId,
                                     Predicate<Node> alive,
                                     String currentLeaderId) {
    if (selfId == null) selfId = "";
    if (nodes == null || nodes.isEmpty()) return selfId;

    Comparator<Node> cmp = Comparator
        .comparingLong((Node n) -> ordinal(n.id()).orElse(Long.MAX_VALUE))
        .thenComparing(Node::id);

    Node best = null;
    Node current = null;
    for (Node n : nodes) {
      if (n == null) continue;

      if (currentLeaderId != null && currentLeaderId.equals(n.id())) {
        current = n;
      }

      boolean ok;
      if (n.id().equals(selfId)) {
        ok = true;
      } else {
        ok = alive != null && alive.test(n);
      }

      if (!ok) continue;
      if (best == null || cmp.compare(n, best) < 0) best = n;
    }

    if (best == null) return selfId;

    // Keep current leader only if it is ALSO the best candidate by deterministic rule.
    // This fixes bootstrap split-brain where each node initially considers itself leader.
    if (current != null && currentLeaderId != null && !currentLeaderId.isBlank()) {
      boolean currentAlive;
      if (current.id().equals(selfId)) {
        currentAlive = true;
      } else {
        currentAlive = alive != null && alive.test(current);
      }
      if (currentAlive && cmp.compare(current, best) == 0) {
        return currentLeaderId;
      }
    }

    return best.id();
  }
}
