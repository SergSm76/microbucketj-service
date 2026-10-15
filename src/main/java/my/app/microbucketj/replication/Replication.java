package my.app.microbucketj.replication;

import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public final class Replication {

  private Replication() {
  }

  public static Plan plan(ClusterView view,
                          List<Node> replicas,
                          TokenPeerClient tokenPeers,
                          QuorumMode mode,
                          int minAcks) {
    List<Node> allTargets = new ArrayList<>(Math.max(0, replicas.size() - 1));
    List<Node> connectedTargets = new ArrayList<>(Math.max(0, replicas.size() - 1));

    for (Node n : replicas) {
      if (n.id().equals(view.self().id())) continue;
      allTargets.add(n);
      if (tokenPeers.isConnected(n)) connectedTargets.add(n);
    }

    int need;
    List<Node> targets;

    switch (mode) {
      case STATIC_MIN_ACKS -> {
        targets = allTargets;
        need = Math.min(Math.max(0, minAcks), allTargets.size());
      }
      case CONNECTED_MAJORITY -> {
        targets = connectedTargets;
        int connected = connectedTargets.size();
        int available = 1 + connected; // self + connected replicas
        int needTotal = (available / 2) + 1; // majority
        need = Math.max(0, needTotal - 1); // need ACKs from replicas
        if (need > connected) need = connected;
      }
      default -> {
        targets = connectedTargets;
        need = Math.min(Math.max(0, minAcks), connectedTargets.size());
      }
    }

    return new Plan(targets, need);
  }

  public static CompletableFuture<Integer> replicatePutUntilQuorum(TokenPeerClient peers,
                                                                   List<Node> targets,
                                                                   int epoch,
                                                                   BucketState st,
                                                                   int need,
                                                                   long ackTimeoutMs) {
    AtomicInteger ok = new AtomicInteger(0);
    AtomicInteger left = new AtomicInteger(0);
    CompletableFuture<Integer> done = new CompletableFuture<>();

    for (Node n : targets) {
      left.incrementAndGet();
      peers.replicatePut(n, epoch, st, ackTimeoutMs)
          .whenComplete((ack, err) -> {
            int oks = ok.get();
            if (err == null && ack != null && ack.ok()) {
              oks = ok.incrementAndGet();
            }

            if (oks >= need) {
              done.complete(oks);
              return;
            }

            if (left.decrementAndGet() == 0) {
              done.complete(ok.get());
            }
          });
    }

    if (left.get() == 0) done.complete(0);
    return done;
  }

  public record Plan(List<Node> targets, int needAcks) {
  }
}
