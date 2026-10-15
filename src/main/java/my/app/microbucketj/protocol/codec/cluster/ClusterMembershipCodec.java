package my.app.microbucketj.protocol.codec.cluster;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.common.WireStrings;

import java.util.ArrayList;
import java.util.List;

public final class ClusterMembershipCodec {

  private ClusterMembershipCodec() {
  }

  public static void writePayload(ByteBuf out, List<Node> nodes, int epoch) {
    out.writeByte(ClusterProtocol.OP_MEMBERSHIP);
    out.writeInt(epoch);
    out.writeShort(nodes.size());
    for (Node node : nodes) {
      WireStrings.writeU8(out, node.id());
      WireStrings.writeU8(out, node.host());
      out.writeInt(node.clusterPort());
    }
  }

  public static MembershipData read(ByteBuf in) {
    int epoch = in.readInt();
    int count = in.readUnsignedShort();
    List<Node> nodes = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String id = WireStrings.readU8(in);
      String host = WireStrings.readU8(in);
      int clusterPort = in.readInt();
      nodes.add(new Node(id, host, clusterPort));
    }
    return new MembershipData(epoch, nodes);
  }

  public record MembershipData(int epoch, List<Node> nodes) {
  }
}
