package my.app.microbucketj.config;

import java.util.ArrayList;

/**
 * Dynamic spec for either a plain rate limiter or an adaptive rate limiter.
 */
public record BucketSpec(
    String name,
    long version,
    BucketType type,
    long capacity,
    long refillTokens,
    long refillPeriodMs,
    long[] stepCapacities,
    long stepPeriodMs,
    long notUsedResetMs,
    long adaptiveWindowMs,
    int adaptiveMinRequests,
    int adaptiveFailureRatePct,
    int adaptiveMinFactorPct,
    int adaptiveDecreaseStepPct,
    int adaptiveIncreaseStepPct
) {

  public BucketSpec(String name, long version, long capacity, long refillTokens, long refillPeriodMs) {
    this(name, version, BucketType.TOKEN,
        capacity, refillTokens, refillPeriodMs,
        null, 0L, 0L,
        0L, 0, 0, 0, 0, 0);
  }

  public BucketSpec(String name, long version, long capacity, long refillTokens, long refillPeriodMs,
                    long[] stepCapacities, long stepPeriodMs, long notUsedResetMs) {
    this(name, version, BucketType.TOKEN,
        capacity, refillTokens, refillPeriodMs,
        stepCapacities, stepPeriodMs, notUsedResetMs,
        0L, 0, 0, 0, 0, 0);
  }

  public BucketSpec {
    type = (type == null) ? BucketType.TOKEN : type;
    if (name == null || name.isBlank()) throw new IllegalArgumentException("Bucket name is blank");
    if (version <= 0) throw new IllegalArgumentException("Bucket version must be > 0");

    if (stepCapacities != null) stepCapacities = stepCapacities.clone();
    if (capacity <= 0 || refillTokens <= 0 || refillPeriodMs <= 0) {
      throw new IllegalArgumentException("Rate limiter must have cap/refill/periodMs > 0");
    }
    if (notUsedResetMs < 0) throw new IllegalArgumentException("notUsedResetMs must be >= 0");
    if (stepCapacities != null && stepCapacities.length > 0) {
      stepCapacities = normalizeStepCapacities(stepCapacities, capacity);
      if (stepPeriodMs <= 0) {
        throw new IllegalArgumentException("stepPeriodMs must be > 0 when stepCapacities is set");
      }
    }

    if (type == BucketType.TOKEN) {
      adaptiveWindowMs = 0L;
      adaptiveMinRequests = 0;
      adaptiveFailureRatePct = 0;
      adaptiveMinFactorPct = 0;
      adaptiveDecreaseStepPct = 0;
      adaptiveIncreaseStepPct = 0;
    } else {
      if (adaptiveWindowMs <= 0) throw new IllegalArgumentException("adaptiveWindowMs must be > 0");
      if (adaptiveMinRequests <= 0) throw new IllegalArgumentException("adaptiveMinRequests must be > 0");
      if (adaptiveFailureRatePct <= 0 || adaptiveFailureRatePct > 100) {
        throw new IllegalArgumentException("adaptiveFailureRatePct must be in 1..100");
      }
      if (adaptiveMinFactorPct <= 0 || adaptiveMinFactorPct > 100) {
        throw new IllegalArgumentException("adaptiveMinFactorPct must be in 1..100");
      }
      if (adaptiveDecreaseStepPct <= 0 || adaptiveDecreaseStepPct > 100) {
        throw new IllegalArgumentException("adaptiveDecreaseStepPct must be in 1..100");
      }
      if (adaptiveIncreaseStepPct <= 0 || adaptiveIncreaseStepPct > 100) {
        throw new IllegalArgumentException("adaptiveIncreaseStepPct must be in 1..100");
      }
    }
  }

  private static long[] normalizeStepCapacities(long[] in, long cap) {
    if (in == null || in.length == 0 || cap <= 0) return null;

    ArrayList<Long> out = new ArrayList<>(in.length + 1);
    long prev = 0;
    for (int i = 0; i < in.length; i++) {
      long v = in[i];
      if (v <= 0) throw new IllegalArgumentException("step-capacities must be > 0");
      if (v > cap) v = cap;
      if (i > 0 && v < prev) throw new IllegalArgumentException("step-capacities must be non-decreasing");
      if (out.isEmpty() || out.get(out.size() - 1) != v) out.add(v);
      prev = v;
      if (v == cap) break;
    }

    if (out.isEmpty()) out.add(cap);
    else if (out.get(out.size() - 1) < cap) out.add(cap);

    long[] arr = new long[out.size()];
    for (int i = 0; i < out.size(); i++) arr[i] = out.get(i);
    return arr;
  }

  public boolean isTokenBucket() {
    return type == BucketType.TOKEN;
  }

  public boolean isAdaptiveRate() {
    return type == BucketType.ADAPTIVE_RATE;
  }

  public boolean stepped() {
    return stepCapacities != null && stepCapacities.length > 0 && stepPeriodMs > 0;
  }

  public long capacityAtStep(int idx) {
    if (!stepped()) return capacity;
    if (idx <= 0) return stepCapacities[0];
    int last = stepCapacities.length - 1;
    if (idx >= last) return stepCapacities[last];
    return stepCapacities[idx];
  }

  public int lastStepIndex() {
    return stepped() ? (stepCapacities.length - 1) : 0;
  }
}
