package my.app.microbucketj.cluster.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.cluster.ClusterMembershipCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import my.app.microbucketj.utils.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class MembershipOp implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MembershipOp.class);

  private final MembershipRuntime membership;
  private final ClusterPeerClient clusterPeers;
  private final TokenPeerClient tokenPeers;
  private final BucketRegistry buckets;
  private final ReplicationService replication;
  private int epoch;

  public MembershipOp(MembershipRuntime membership,
                      ClusterPeerClient clusterPeers,
                      TokenPeerClient tokenPeers,
                      BucketRegistry buckets,
                      ReplicationService replication) {
    this.membership = Objects.requireNonNull(membership, "membership");
    this.clusterPeers = Objects.requireNonNull(clusterPeers, "clusterPeers");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
    this.buckets = Objects.requireNonNull(buckets, "buckets");
    this.replication = Objects.requireNonNull(replication, "replication");
    this.epoch = membership.snapshot().epoch();
  }

  static List<Node> stateTransferTargets(ClusterView oldView,
                                         ClusterView newView,
                                         long bucketHash) {
    if (oldView == null || newView == null) return List.of();

    Node selfOld = oldView.self();
    Node oldPrimary = oldView.pickPrimary(bucketHash);
    if (!oldPrimary.id().equals(selfOld.id())) return List.of();

    Node selfNew = newView.self();
    ArrayList<Node> targets = new ArrayList<>();
    for (Node node : newView.pickReplicas(bucketHash)) {
      if (!node.id().equals(selfNew.id())) {
        targets.add(node);
      }
    }
    return targets;
  }

  @Override
  public byte opcode() {
    return ClusterProtocol.OP_MEMBERSHIP;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    onMembership(in);
  }

  private void onMembership(ByteBuf in) {
    ClusterMembershipCodec.MembershipData data = ClusterMembershipCodec.read(in);
    int newEpoch = data.epoch();
    List<Node> nodes = data.nodes();

    var oldSnap = membership.snapshot();
    membership.applyMembership(newEpoch, nodes);
    var newSnap = membership.snapshot();

    this.epoch = Math.max(this.epoch, newEpoch);

    Node self = newSnap.view().self();
    clusterPeers.connectAll(nodes, self);
    tokenPeers.connectAll(nodes, self);

    logOwnership(oldSnap, newSnap);
    doHandoff(oldSnap, newSnap);
  }

  private void logOwnership(MembershipRuntime.Snapshot oldSnap, MembershipRuntime.Snapshot newSnap) {
    try {
      var oldView = oldSnap.view();
      var newView = newSnap.view();

      Node selfNew = newView.self();

      ArrayList<String> owned = new ArrayList<>();
      for (String bucket : buckets.bucketNames()) {
        byte[] bucketBytes = bucket.getBytes(StandardCharsets.UTF_8);
        long bucketHash = Hashing.fnv1a64(bucketBytes, 0, bucketBytes.length);

        Node oldPrimary = oldView.pickPrimary(bucketHash);
        Node newPrimary = newView.pickPrimary(bucketHash);

        if (!oldPrimary.id().equals(newPrimary.id())) {
          LOG.info("Основной бакет изменён: bucket={} {} -> {}", bucket, oldPrimary.id(), newPrimary.id());
        }
        if (newPrimary.id().equals(selfNew.id())) {
          owned.add(bucket);
        }
      }

      int limit = 50;
      List<String> sample = (owned.size() <= limit) ? owned : owned.subList(0, limit);
      LOG.info("Владелец бакета: self={} owns={} sample={}", selfNew.id(), owned.size(), sample);
    } catch (Exception e) {
      // best-effort logging only
    }
  }

  private void doHandoff(MembershipRuntime.Snapshot oldSnap, MembershipRuntime.Snapshot newSnap) {
    if (!replication.isEnabled()) return;

    var oldView = oldSnap.view();
    var newView = newSnap.view();

    Node selfNew = newView.self();

    for (String bucket : buckets.bucketNames()) {
      byte[] bucketBytes = bucket.getBytes(StandardCharsets.UTF_8);
      long bucketHash = Hashing.fnv1a64(bucketBytes, 0, bucketBytes.length);

      List<Node> targets = stateTransferTargets(oldView, newView, bucketHash);
      if (targets.isEmpty()) continue;

      AbstractBucketRuntime rt = buckets.get(bucket);
      if (rt == null) continue;
      BucketState st = rt.snapshot();

      for (Node n : targets) {
        if (n.id().equals(selfNew.id())) continue;
        tokenPeers.stateTransfer(n, newSnap.epoch(), st);
      }
    }
  }
}
