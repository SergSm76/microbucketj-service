package my.app.microbucketj.protocol;

public final class TokenProtocol {

  public static final byte CURRENT_VERSION = ProtocolVersion.CURRENT;

  public static final byte OP_FWD_RESP = 0x12;
  public static final byte OP_PING = 0x13;
  public static final byte OP_PONG = 0x14;

  public static final byte OP_FWD_ACQUIRE_HASH = 0x15;
  public static final byte OP_FWD_ACQUIRE_NAME = 0x16;
  public static final byte OP_FWD_REPORT_HASH = 0x17;
  public static final byte OP_FWD_REPORT_NAME = 0x18;

  public static final byte OP_REPL_PUT = 0x20;
  public static final byte OP_REPL_ACK = 0x21;
  public static final byte OP_REPL_PUT_BATCH = 0x22;
  public static final byte OP_STATE_XFER = 0x40;

  public static final int FWD_RESP_PAYLOAD_LEN = 1 + 1 + 4 + 8 + 8;
  public static final int REPL_ACK_PAYLOAD_LEN = 1 + 4 + 1;
  public static final int PING_PONG_PAYLOAD_LEN = 1 + 8;

  public static final byte ST_DENIED = 0;
  public static final byte ST_ALLOWED = 1;
  public static final byte ST_UNAVAILABLE = 3;
  public static final byte ST_NOT_FOUND = 4;
  public static final byte ST_OK = 5;
  public static final byte ST_COLLISION = 7;
  public static final byte ST_SHEDDED = 8;
  public static final byte ST_BREAKER_OPEN = 9;

  private TokenProtocol() {
  }
}
