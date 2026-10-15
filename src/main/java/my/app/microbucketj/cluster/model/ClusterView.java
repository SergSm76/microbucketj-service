package my.app.microbucketj.cluster.model;

import my.app.microbucketj.utils.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class ClusterView {
  private final Node self;
  private final NodeWithHash[] nodes;
  private final int replicaCount;

  public ClusterView(String selfId, List<Node> nodes, int replicaCount) {
    Objects.requireNonNull(selfId, "selfId");
    if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("nodes empty");
    if (replicaCount <= 0) throw new IllegalArgumentException("replicaCount must be > 0");

    this.replicaCount = replicaCount;

    Node selfNode = null;
    NodeWithHash[] tmp = new NodeWithHash[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      Node n = nodes.get(i);
      if (n.id().equals(selfId)) selfNode = n;

      String s = n.id() + "@" + n.clusterAddrKey();
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      long nh = Hashing.fnv1a64(b, 0, b.length);

      tmp[i] = new NodeWithHash(n, nh);
    }
    if (selfNode == null) throw new IllegalStateException("selfId not found: " + selfId);

    this.self = selfNode;
    this.nodes = tmp;
  }

  public Node self() {
    return self;
  }

  public Node pickPrimary(long keyHash) {
    Node best = null;
    long bestScore = Long.MIN_VALUE;
    for (NodeWithHash n : nodes) {
      long s = Hashing.score(keyHash, n.nodeHash);
      if (s > bestScore) {
        bestScore = s;
        best = n.node;
      }
    }
    return best;
  }


  public List<Node> pickReplicas(long keyHash) {
    int k = Math.min(replicaCount, nodes.length);
    Node[] bestNodes = new Node[k];
    long[] bestScores = new long[k];
    Arrays.fill(bestScores, Long.MIN_VALUE);

    for (NodeWithHash n : nodes) {
      long s = Hashing.score(keyHash, n.nodeHash);

      // quick reject
      if (s <= bestScores[k - 1]) continue;

      // insert into sorted (desc) top-K arrays
      int pos = k - 1;
      while (pos > 0 && s > bestScores[pos - 1]) {
        bestScores[pos] = bestScores[pos - 1];
        bestNodes[pos] = bestNodes[pos - 1];
        pos--;
      }
      bestScores[pos] = s;
      bestNodes[pos] = n.node;
    }

    ArrayList<Node> out = new ArrayList<>(k);
    for (Node n : bestNodes) {
      if (n != null) out.add(n);
    }
    return out;
  }

  private record NodeWithHash(Node node, long nodeHash) {
  }
}
