package my.app.microbucketj.protocol.codec.cluster;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.common.WireStrings;

public final class ClusterJoinCodec {

  private ClusterJoinCodec() {
  }

  public static void writePayload(ByteBuf out, Node self) {
    out.writeByte(ClusterProtocol.OP_JOIN);
    WireStrings.writeU8(out, self.id());
    WireStrings.writeU8(out, self.host());
    out.writeInt(self.clusterPort());
  }

  public static Node readNode(ByteBuf in) {
    String nodeId = WireStrings.readU8(in);
    String host = WireStrings.readU8(in);
    int clusterPort = in.readInt();
    return new Node(nodeId, host, clusterPort);
  }
}
