package my.app.microbucketj.protocol.codec.internal;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.ProtocolVersion;
import my.app.microbucketj.protocol.codec.common.WireStrings;

public final class InternalHelloCodec {

  private InternalHelloCodec() {
  }

  public static void writePayload(ByteBuf out, byte role, String nodeId) {
    out.writeByte(InternalProtocol.OP_HELLO);
    out.writeByte(ProtocolVersion.VERSIONED_MARKER);
    out.writeByte(InternalProtocol.CURRENT_VERSION);
    out.writeByte(role);
    WireStrings.writeU8(out, nodeId == null ? "" : nodeId);
  }

  public static HelloData read(ByteBuf in) {
    byte op = in.readByte();
    if (op != InternalProtocol.OP_HELLO) {
      throw new IllegalArgumentException("missing HELLO op: " + op);
    }
    if (!in.isReadable()) {
      throw new IllegalArgumentException("missing HELLO body");
    }

    byte first = in.readByte();
    if (first == ProtocolVersion.VERSIONED_MARKER) {
      if (in.readableBytes() < 2) {
        throw new IllegalArgumentException("missing HELLO version/role");
      }
      byte version = in.readByte();
      ProtocolVersion.requireSupported(version);
      byte role = in.readByte();
      String nodeId = in.isReadable() ? WireStrings.readU8(in) : "";
      return new HelloData(version, role, nodeId, true);
    }

    byte legacyRole = first;
    String nodeId = in.isReadable() ? WireStrings.readU8(in) : "";
    return new HelloData(ProtocolVersion.V1, legacyRole, nodeId, false);
  }

  public record HelloData(byte protocolVersion, byte role, String nodeId, boolean versioned) {
  }
}
