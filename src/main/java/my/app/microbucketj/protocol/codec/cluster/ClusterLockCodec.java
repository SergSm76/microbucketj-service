package my.app.microbucketj.protocol.codec.cluster;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;

public final class ClusterLockCodec {
  private ClusterLockCodec() {
  }

  public static void writeUpsertPayload(ByteBuf out, LockEntry entry) {
    out.writeByte(ClusterProtocol.OP_LOCK_UPSERT);
    ApiLockCodec.writeEntryPayload(out, entry);
  }

  public static void writeDeletePayload(ByteBuf out, String name) {
    out.writeByte(ClusterProtocol.OP_LOCK_DELETE);
    ApiLockCodec.writeNamePayload(out, name);
  }
}