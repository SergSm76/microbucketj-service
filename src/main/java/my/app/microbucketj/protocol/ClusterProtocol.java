package my.app.microbucketj.protocol;

public final class ClusterProtocol {

  public static final byte CURRENT_VERSION = ProtocolVersion.CURRENT;

  public static final byte OP_JOIN = 0x34;
  public static final byte OP_MEMBERSHIP = 0x35;

  public static final byte OP_PING = 0x32;
  public static final byte OP_PONG = 0x33;

  public static final byte OP_BUCKET_UPSERT = 0x36;

  public static final byte OP_LOCK_UPSERT = 0x37;  // broadcast записи блокировки
  public static final byte OP_LOCK_DELETE = 0x38;  // broadcast удаления блокировки

  private ClusterProtocol() {
  }
}
