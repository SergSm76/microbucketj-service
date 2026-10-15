package my.app.microbucketj.bucket;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.protocol.ProtocolVersion;
import my.app.microbucketj.protocol.codec.common.WireStrings;

/**
 * Compact wire codec for replicated bucket state.
 */
public final class BucketStateWire {

  private BucketStateWire() {
  }

  public static void write(ByteBuf out, BucketState st) {
    WireStrings.writeU8(out, st.bucket());
    out.writeByte(ProtocolVersion.CURRENT);
    out.writeLong(st.specVersion());
    out.writeLong(st.seq());
    BucketType type = st.type();
    out.writeByte(type == BucketType.ADAPTIVE_RATE ? 1 : 0);

    out.writeLong(st.tokens());
    out.writeLong(st.lastRefillNanos());
    out.writeLong(st.lastAccessNanos());
    out.writeInt(st.stepIndex());
    out.writeLong(st.stepStartNanos());

    if (type == BucketType.ADAPTIVE_RATE) {
      out.writeInt(st.adaptiveRatePct());
      out.writeLong(st.adaptiveWindowStartNanos());
      out.writeInt(st.adaptiveWindowRequests());
      out.writeInt(st.adaptiveWindowFailures());
      out.writeInt(st.adaptiveGoodWindows());
    }
  }

  public static BucketState read(ByteBuf in) {
    String bucket = WireStrings.readU8(in);
    if (!in.isReadable()) throw new IllegalArgumentException("missing bucket-state version");
    byte wireVersion = in.readByte();
    ProtocolVersion.requireSupported(wireVersion);

    long specVersion = in.readLong();
    long seq = in.readLong();
    BucketType type = in.readByte() == 1 ? BucketType.ADAPTIVE_RATE : BucketType.TOKEN;

    long tokens = in.readLong();
    long lastRefill = in.readLong();
    long lastAccess = in.readLong();
    int stepIndex = in.readInt();
    long stepStart = in.readLong();

    if (type == BucketType.ADAPTIVE_RATE) {
      int adaptiveRatePct = in.readInt();
      long adaptiveWindowStart = in.readLong();
      int adaptiveWindowRequests = in.readInt();
      int adaptiveWindowFailures = in.readInt();
      int adaptiveGoodWindows = in.readInt();
      return new BucketState(bucket, specVersion, seq, type,
          tokens, lastRefill, lastAccess, stepIndex, stepStart,
          adaptiveRatePct, adaptiveWindowStart, adaptiveWindowRequests, adaptiveWindowFailures, adaptiveGoodWindows);
    }

    return new BucketState(bucket, specVersion, seq, type,
        tokens, lastRefill, lastAccess, stepIndex, stepStart,
        0, 0L, 0, 0, 0);
  }
}
