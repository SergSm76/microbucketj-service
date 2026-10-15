package my.app.microbucketj.protocol;

public final class InternalProtocol {

  public static final byte CURRENT_VERSION = ProtocolVersion.CURRENT;

  public static final byte OP_HELLO = 0x01;

  public static final byte ROLE_TOKEN = 0x01;
  public static final byte ROLE_CLUSTER = 0x02;

  private InternalProtocol() {
  }
}
