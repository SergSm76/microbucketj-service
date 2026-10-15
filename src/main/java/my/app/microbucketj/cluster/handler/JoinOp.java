package my.app.microbucketj.cluster.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.cluster.ClusterJoinCodec;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;

public final class JoinOp implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JoinOp.class);

  private final BooleanSupplier isLeader;
  private final MembershipRuntime membership;
  private final ClusterPeerClient clusterPeers;
  private final TokenPeerClient tokenPeers;
  private int epoch;

  public JoinOp(BooleanSupplier isLeader,
                MembershipRuntime membership,
                ClusterPeerClient clusterPeers,
                TokenPeerClient tokenPeers) {
    this.isLeader = (isLeader == null) ? (() -> false) : isLeader;
    this.membership = Objects.requireNonNull(membership, "membership");
    this.clusterPeers = Objects.requireNonNull(clusterPeers, "clusterPeers");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
    this.epoch = membership.snapshot().epoch();
  }

  @Override
  public byte opcode() {
    return ClusterProtocol.OP_JOIN;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (!isLeader.getAsBoolean() && membership.snapshot().nodes().size() > 1) return;
    onJoin(in);
  }

  private synchronized void onJoin(ByteBuf in) {
    Node newNode = ClusterJoinCodec.readNode(in);
    var snap = membership.snapshot();
    List<Node> nodes = new ArrayList<>(snap.nodes());
    boolean changed = false;

    int byId = -1;
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i).id().equals(newNode.id())) {
        byId = i;
        break;
      }
    }

    if (byId >= 0) {
      Node old = nodes.get(byId);
      if (!old.equals(newNode)) {
        nodes.set(byId, newNode);
        changed = true;
      }
    } else {
      boolean addrClash = nodes.stream().anyMatch(n -> n.clusterAddrKey().equals(newNode.clusterAddrKey()));
      if (!addrClash) {
        nodes.add(newNode);
        changed = true;
      }
    }

    Node self = membership.snapshot().view().self();
    this.epoch = Math.max(this.epoch, membership.snapshot().epoch());

    if (changed) {
      epoch++;
      membership.applyMembership(epoch, nodes);

      LOG.info("Join применен: nodeId={} host={} port={} epoch={} size={}",
          newNode.id(), newNode.host(), newNode.clusterPort(), epoch, nodes.size());

      clusterPeers.connectAll(nodes, self);
      tokenPeers.connectAll(nodes, self);
      clusterPeers.broadcastMembership(nodes, epoch, self);
    } else {
      clusterPeers.connect(newNode);
      tokenPeers.connect(newNode);
      clusterPeers.sendMembershipTo(newNode, nodes, epoch);

      LOG.debug("Join обновлен: nodeId={} -> send current membership epoch={} size={}",
          newNode.id(), epoch, nodes.size());
    }
  }
}
