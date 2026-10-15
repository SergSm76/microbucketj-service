package my.app.microbucketj.protocol.codec.common;

import io.netty.buffer.ByteBuf;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.protocol.ProtocolVersion;

public final class BucketSpecWireCodec {

  private BucketSpecWireCodec() {
  }

  public static BucketType readType(ByteBuf in) {
    if (in.readableBytes() < 1) return null;
    int v = in.readUnsignedByte();
    return switch (v) {
      case 0 -> BucketType.TOKEN;
      case 1 -> BucketType.ADAPTIVE_RATE;
      default -> null;
    };
  }

  private static byte readWireVersionIfPresent(ByteBuf in) {
    if (in.readableBytes() >= 2 && in.getByte(in.readerIndex()) == ProtocolVersion.VERSIONED_MARKER) {
      in.readByte();
      byte version = in.readByte();
      ProtocolVersion.requireSupported(version);
      return version;
    }
    return ProtocolVersion.V1;
  }

  private static void writeWireVersion(ByteBuf out) {
    out.writeByte(ProtocolVersion.VERSIONED_MARKER);
    out.writeByte(ProtocolVersion.CURRENT);
  }

  public static void writeTypeAndReserved(ByteBuf out, BucketSpec spec) {
    out.writeByte(spec != null && spec.type() == BucketType.ADAPTIVE_RATE ? 1 : 0);
    out.writeByte(0);
  }

  public static BucketSpec readSpecWithoutName(String bucket, ByteBuf in) {
    readWireVersionIfPresent(in);

    BucketType type = readType(in);
    if (type == null || in.readableBytes() < 1) return null;
    in.readUnsignedByte(); // reserved

    long ver = in.readLong();
    long cap = in.readLong();
    long refill = in.readLong();
    long periodMs = in.readLong();
    long stepPeriodMs = in.readLong();
    long notUsedMs = in.readLong();
    int stepCount = in.readUnsignedByte();
    long[] steps = null;
    if (stepCount > 0) {
      steps = new long[stepCount];
      for (int i = 0; i < stepCount; i++) steps[i] = in.readLong();
    }

    if (type == BucketType.TOKEN) {
      return new BucketSpec(bucket, ver, type,
          cap, refill, periodMs, steps, stepPeriodMs, notUsedMs,
          0L, 0, 0, 0, 0, 0);
    }

    long adaptiveWindowMs = in.readLong();
    int adaptiveMinRequests = in.readInt();
    int adaptiveFailureRatePct = in.readInt();
    int adaptiveMinFactorPct = in.readInt();
    int adaptiveDecreaseStepPct = in.readInt();
    int adaptiveIncreaseStepPct = in.readInt();

    return new BucketSpec(bucket, ver, type,
        cap, refill, periodMs, steps, stepPeriodMs, notUsedMs,
        adaptiveWindowMs, adaptiveMinRequests, adaptiveFailureRatePct,
        adaptiveMinFactorPct, adaptiveDecreaseStepPct, adaptiveIncreaseStepPct);
  }

  public static void writeSpecWithoutName(ByteBuf out, BucketSpec spec) {
    writeWireVersion(out);
    writeTypeAndReserved(out, spec);
    out.writeLong(spec.version());
    long[] steps = spec.stepCapacities();
    int stepCount = (steps == null) ? 0 : Math.min(255, steps.length);
    out.writeLong(spec.capacity());
    out.writeLong(spec.refillTokens());
    out.writeLong(spec.refillPeriodMs());
    out.writeLong(spec.stepPeriodMs());
    out.writeLong(spec.notUsedResetMs());
    out.writeByte(stepCount);
    for (int i = 0; i < stepCount; i++) out.writeLong(steps[i]);
    if (spec.isAdaptiveRate()) {
      out.writeLong(spec.adaptiveWindowMs());
      out.writeInt(spec.adaptiveMinRequests());
      out.writeInt(spec.adaptiveFailureRatePct());
      out.writeInt(spec.adaptiveMinFactorPct());
      out.writeInt(spec.adaptiveDecreaseStepPct());
      out.writeInt(spec.adaptiveIncreaseStepPct());
    }
  }

  public static void writeFixedAdminSpec(ByteBuf out, BucketSpec spec) {
    writeWireVersion(out);
    if (spec == null) {
      out.writeByte(0).writeByte(0);
      for (int i = 0; i < 7; i++) out.writeLong(0L);
      for (int i = 0; i < 5; i++) out.writeInt(0);
      return;
    }
    writeTypeAndReserved(out, spec);
    out.writeLong(spec.version());
    out.writeLong(spec.capacity());
    out.writeLong(spec.refillTokens());
    out.writeLong(spec.refillPeriodMs());
    out.writeLong(spec.stepPeriodMs());
    out.writeLong(spec.notUsedResetMs());
    out.writeLong(spec.adaptiveWindowMs());
    out.writeInt(spec.adaptiveMinRequests());
    out.writeInt(spec.adaptiveFailureRatePct());
    out.writeInt(spec.adaptiveMinFactorPct());
    out.writeInt(spec.adaptiveDecreaseStepPct());
    out.writeInt(spec.adaptiveIncreaseStepPct());
  }
}
