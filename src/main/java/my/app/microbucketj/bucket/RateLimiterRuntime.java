package my.app.microbucketj.bucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.protocol.ApiProtocol;

import static my.app.microbucketj.utils.Constants.ONE_PERMIT;

public final class RateLimiterRuntime extends AbstractBucketRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(RateLimiterRuntime.class);
  private static final ReportResult REPORT_NOOP = new ReportResult(ApiProtocol.ST_OK, false);
  private static final long ACCESS_UPDATE_MIN_NANOS = 1_000_000L; // 1ms

  private long seq;

  private long tokens;
  private long lastRefillNanos;
  private long refillRemainder;

  private long lastAccessNanos;

  private int stepIdx;
  private long stepStartNanos;

  private long warmupSpecVersion;
  private long warmupNotUsedResetNanos;
  private long refillPeriodNanos;
  private long stepPeriodNanos;

  public RateLimiterRuntime(BucketSpec spec) {
    super(spec);
    long now = System.nanoTime();
    this.seq = 0;
    this.stepIdx = 0;
    this.stepStartNanos = now;
    refreshSpecCaches(spec);
    this.tokens = effectiveCapacity(spec, stepIdx);
    this.lastRefillNanos = now;
    this.refillRemainder = 0;
    this.lastAccessNanos = now;
  }

  private static long effectiveCapacity(BucketSpec s, int stepIdx) {
    if (s == null) return 0L;
    if (!s.stepped()) return s.capacity();
    return s.capacityAtStep(stepIdx);
  }

  private static long effectiveRateTokens(BucketSpec s, int stepIdx) {
    long rate = s.refillTokens();
    if (rate <= 0) rate = 1;
    if (s.stepped()) {
      long capStep = s.capacityAtStep(stepIdx);
      if (capStep > 0 && capStep < rate) rate = capStep;
    }
    return rate;
  }

  private static long waitNanosForTokens(long needTokens, long periodNanos, long rateTokens, long remainder) {
    if (needTokens <= 0) return 0;
    if (rateTokens <= 0) rateTokens = 1;
    long target = (needTokens > Long.MAX_VALUE / periodNanos) ? Long.MAX_VALUE : needTokens * periodNanos;
    long missing = target - remainder;
    if (missing <= 0) return 0;
    return (missing + rateTokens - 1) / rateTokens;
  }

  private static long clampRetryMs(long waitNanos) {
    return (waitNanos <= 0) ? 0 : ((waitNanos + 999_999L) / 1_000_000L);
  }

  @Override
  public synchronized boolean updateSpec(BucketSpec newSpec) {
    if (newSpec == null || !newSpec.isTokenBucket()) return false;
    for (; ; ) {
      BucketSpec cur = getVolatileSpec();
      if (!newSpec.name().equals(cur.name())) return false;
      if (!cur.isTokenBucket() || newSpec.version() <= cur.version()) return false;
      if (compareAndSetSpec(cur, newSpec)) return true;
    }
  }

  @Override
  public synchronized AcquireResult acquireAsPrimary(long permits) {
    if (permits <= 0) permits = ONE_PERMIT;
    if (permits == ONE_PERMIT) return acquire1AsPrimary();

    long now = System.nanoTime();
    onSpecChangedIfNeeded(now);
    stepTick(now);

    BucketSpec s = spec();
    refillIfNeededContinuous(now, s);

    long cap = effectiveCapacity(s, stepIdx);
    if (tokens > cap) tokens = cap;

    byte status;
    long retryMs;

    if (tokens >= permits) {
      tokens -= permits;
      status = ApiProtocol.ST_ALLOWED;
      retryMs = 0;
    } else {
      status = ApiProtocol.ST_DENIED;
      long rateTokens = effectiveRateTokens(s, stepIdx);
      long periodNanos = refillPeriodNanos;
      long need = permits - tokens;
      long waitNanos = waitNanosForTokens(need, periodNanos, rateTokens, refillRemainder);
      retryMs = clampRetryMs(waitNanos);
    }

    touchAccess(now);
    seq++;
    return new AcquireResult(status, tokens, retryMs, true);
  }

  @Override
  public synchronized AcquireResult acquire1AsPrimary() {
    long now = System.nanoTime();
    onSpecChangedIfNeeded(now);
    stepTick(now);

    BucketSpec s = spec();
    refillIfNeededContinuous(now, s);

    long cap = effectiveCapacity(s, stepIdx);
    if (tokens > cap) tokens = cap;

    byte status;
    long retryMs;

    if (tokens > 0) {
      tokens -= ONE_PERMIT;
      status = ApiProtocol.ST_ALLOWED;
      retryMs = 0;
    } else {
      status = ApiProtocol.ST_DENIED;
      long rateTokens = effectiveRateTokens(s, stepIdx);
      long periodNanos = refillPeriodNanos;
      long waitNanos = waitNanosForTokens(ONE_PERMIT, periodNanos, rateTokens, refillRemainder);
      retryMs = clampRetryMs(waitNanos);
    }

    touchAccess(now);
    seq++;
    return new AcquireResult(status, tokens, retryMs, true);
  }

  @Override
  public synchronized ReportResult reportAsPrimary(boolean success) {
    return REPORT_NOOP;
  }

  @Override
  public synchronized void applyReplica(BucketState s) {
    if (s == null || s.type() != BucketType.TOKEN) return;
    if (s.specVersion() != spec().version()) return;
    if (s.seq() <= seq) return;
    seq = s.seq();
    tokens = s.tokens();
    lastRefillNanos = s.lastRefillNanos();
    lastAccessNanos = s.lastAccessNanos();
    stepIdx = s.stepIndex();
    stepStartNanos = s.stepStartNanos();
    refillRemainder = 0;
    refreshSpecCaches(spec());
  }

  @Override
  public synchronized BucketState snapshot() {
    BucketSpec s = spec();
    return new BucketState(
        s.name(), s.version(), seq, BucketType.TOKEN,
        tokens, lastRefillNanos, lastAccessNanos, stepIdx, stepStartNanos,
        0, 0L, 0, 0, 0
    );
  }

  @Override
  public synchronized long seq() {
    return seq;
  }

  private void refillIfNeededContinuous(long now, BucketSpec s) {
    if (seq == 0) {
      lastRefillNanos = now;
      refillRemainder = 0;
      return;
    }
    long periodNanos = refillPeriodNanos;
    long elapsed = now - lastRefillNanos;
    if (elapsed <= 0) return;

    long cap = effectiveCapacity(s, stepIdx);
    if (tokens >= cap) {
      tokens = cap;
      lastRefillNanos = now;
      refillRemainder = 0;
      return;
    }

    long rateTokens = effectiveRateTokens(s, stepIdx);
    long fullPeriods = elapsed / periodNanos;
    long remNanos = elapsed - fullPeriods * periodNanos;
    long add = fullPeriods * rateTokens;
    long fracNumer = remNanos * rateTokens + refillRemainder;
    add += fracNumer / periodNanos;
    refillRemainder = fracNumer % periodNanos;

    long nt = tokens + add;
    tokens = Math.min(nt, cap);
    lastRefillNanos = now;
    if (tokens >= cap) {
      tokens = cap;
      refillRemainder = 0;
    }
  }

  private void touchAccess(long now) {
    if (now - lastAccessNanos >= ACCESS_UPDATE_MIN_NANOS) {
      lastAccessNanos = now;
    }
  }


  private void refreshSpecCaches(BucketSpec s) {
    warmupSpecVersion = s.version();
    warmupNotUsedResetNanos = Math.max(0L, s.notUsedResetMs()) * 1_000_000L;
    refillPeriodNanos = Math.max(1L, s.refillPeriodMs()) * 1_000_000L;
    stepPeriodNanos = Math.max(0L, s.stepPeriodMs()) * 1_000_000L;
  }

  private void onSpecChangedIfNeeded(long now) {
    BucketSpec s = spec();
    long v = s.version();
    if (v == warmupSpecVersion) return;

    long newResetNanos = Math.max(0L, s.notUsedResetMs()) * 1_000_000L;
    long resetNanos = Math.max(warmupNotUsedResetNanos, newResetNanos);
    long idle = now - lastAccessNanos;
    boolean hot = seq > 0 && resetNanos > 0 && idle < resetNanos;

    if (hot) {
      if (s.stepped()) {
        int last = s.lastStepIndex();
        if (stepIdx != last) {
          LOG.info("Шаг бакета проигнорирован из-за обновления спецификации во время активного окна: bucket={} fromStep={} toStep={} cap={}",
              s.name(), stepIdx, last, s.capacity());
        }
        stepIdx = last;
      } else {
        stepIdx = 0;
      }
      stepStartNanos = now;
      long cap = effectiveCapacity(s, stepIdx);
      if (tokens > cap) tokens = cap;
    } else {
      stepIdx = 0;
      stepStartNanos = now;
      tokens = 0;
      lastRefillNanos = now;
      refillRemainder = 0;
    }

    refreshSpecCaches(s);
  }

  private void stepTick(long now) {
    BucketSpec s = spec();
    if (!s.stepped()) return;

    long resetNanos = warmupNotUsedResetNanos;
    if (resetNanos > 0) {
      long idle = now - lastAccessNanos;
      if (idle >= resetNanos) {
        if (stepIdx != 0) {
          LOG.info("Шаг бакета сброшен: bucket={} idleMs={} resetMs={} fromStep={} toStep=0",
              s.name(), idle / 1_000_000L, resetNanos / 1_000_000L, stepIdx);
        }
        stepIdx = 0;
        stepStartNanos = now;
        tokens = 0;
        lastRefillNanos = now;
        refillRemainder = 0;
        return;
      }
    }

    long periodNanos = stepPeriodNanos;
    if (periodNanos <= 0) return;
    long elapsed = now - stepStartNanos;
    if (elapsed < periodNanos) return;

    long stepsToAdvance = elapsed / periodNanos;
    if (stepsToAdvance <= 0) return;

    int last = s.lastStepIndex();
    int newIdx = stepIdx + (int) Math.min(Integer.MAX_VALUE, stepsToAdvance);
    if (newIdx > last) newIdx = last;

    if (newIdx != stepIdx) {
      long fromCap = s.capacityAtStep(stepIdx);
      long toCap = s.capacityAtStep(newIdx);
      LOG.info("Шаг бакета обновлен: bucket={} step {} -> {} cap {} -> {}", s.name(), stepIdx, newIdx, fromCap, toCap);
      stepIdx = newIdx;
      stepStartNanos = (newIdx == last) ? now : (stepStartNanos + stepsToAdvance * periodNanos);
      long cap = s.capacityAtStep(stepIdx);
      if (tokens > cap) tokens = cap;
    }
  }
}
