package my.app.microbucketj.protocol;

public final class ProtocolVersion {

  /**
   * Marker used in variable-length control-plane payloads to distinguish
   * versioned frames from the legacy unversioned layout.
   */
  public static final byte VERSIONED_MARKER = 0x7F;
  public static final byte V1 = 1;
  public static final byte CURRENT = V1;
  public static final byte MIN_SUPPORTED = V1;

  private ProtocolVersion() {
  }

  public static boolean isSupported(byte version) {
    return version >= MIN_SUPPORTED && version <= CURRENT;
  }

  public static void requireSupported(byte version) {
    if (!isSupported(version)) {
      throw new IllegalArgumentException("unsupported protocol version: " + version);
    }
  }
}
