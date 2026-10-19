package my.app.microbucketj.protocol;

public final class InternalProtocol {

  public static final byte CURRENT_VERSION = ProtocolVersion.CURRENT;

  public static final byte OP_HELLO = 0x01;

  public static final byte ROLE_TOKEN = 0x01;
  public static final byte ROLE_CLUSTER = 0x02;

  // Lock operations (internal protocol between nodes)
  public static final byte OP_LOCK_ACQUIRE = 0x50;
  public static final byte OP_LOCK_RELEASE = 0x51;
  public static final byte OP_LOCK_UPDATE = 0x52;
  public static final byte OP_LOCK_DELETE = 0x53;
  public static final byte OP_LOCK_REPL = 0x54;        // Lock replication request
  public static final byte OP_LOCK_FWD_RESP = 0x55;    // Lock forward response
  public static final byte OP_LOCK_REPL_ACK = 0x56;    // Lock replication ack

  private InternalProtocol() {
  }
}
