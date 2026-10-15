package my.app.microbucketj.bucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.protocol.ApiProtocol;

import static my.app.microbucketj.utils.Constants.ONE_PERMIT;

/**
 * Adaptive rate limiter: keeps token bucket semantics, but dynamically lowers/refills
 * the effective rate on failures and slowly restores it on good windows. Warmup still applies.
 */
public final class AdaptiveRateRuntime extends AbstractBucketRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(AdaptiveRateRuntime.class);
  private static final ReportResult REPORT_NOOP = new ReportResult(ApiProtocol.ST_OK, false);
  private static final long MIN_RETRY_MS = 0;
  private static final long ACCESS_UPDATE_MIN_NANOS = 1_000_000L;
  private static final int REPORT_REPL_EVERY = 16;
  private static final int GOOD_WINDOWS_TO_INCREASE = 2;

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
  private long adaptiveWindowNanos;

  private int adaptiveRatePct;
  private long adaptiveWindowStartNanos;
  private int adaptiveWindowRequests;
  private int adaptiveWindowFailures;
  private int adaptiveGoodWindows;
  private int reportsSinceReplica;

  public AdaptiveRateRuntime(BucketSpec spec) {
    super(spec);
    long now = System.nanoTime();
    this.seq = 0;
    this.stepIdx = 0;
    this.stepStartNanos = now;
    this.adaptiveRatePct = 100;
    refreshSpecCaches(spec);
    this.tokens = effectiveCapacity(spec, stepIdx, adaptiveRatePct);
    this.lastRefillNanos = now;
    this.refillRemainder = 0;
    this.lastAccessNanos = now;
    this.adaptiveWindowStartNanos = now;
  }

  private static long effectiveCapacity(BucketSpec s, int stepIdx, int adaptiveRatePct) {
    if (s == null) return 0L;
    long base = s.stepped() ? s.capacityAtStep(stepIdx) : s.capacity();
    long adjusted = (base * Math.max(1, adaptiveRatePct)) / 100L;
    return Math.max(1L, adjusted);
  }

  private static long effectiveRateTokens(BucketSpec s, int stepIdx, int adaptiveRatePct) {
    long rate = s.refillTokens();
    if (rate <= 0) rate = 1;
    if (s.stepped()) {
      long capStep = s.capacityAtStep(stepIdx);
      if (capStep > 0 && capStep < rate) rate = capStep;
    }
    long adjusted = (rate * Math.max(1, adaptiveRatePct)) / 100L;
    return Math.max(1L, adjusted);
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
    long ms = (waitNanos <= 0) ? 0 : ((waitNanos + 999_999L) / 1_000_000L);
    return (MIN_RETRY_MS <= 0) ? ms : Math.max(MIN_RETRY_MS, ms); //TODO always true
  }

  @Override
  public synchronized boolean updateSpec(BucketSpec newSpec) {
    if (newSpec == null || !newSpec.isAdaptiveRate()) return false;
    for (; ; ) {
      BucketSpec cur = getVolatileSpec();
      if (!newSpec.name().equals(cur.name())) return false;
      if (!cur.isAdaptiveRate() || newSpec.version() <= cur.version()) return false;
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

    long cap = effectiveCapacity(s, stepIdx, adaptiveRatePct);
    if (tokens > cap) tokens = cap;

    byte status;
    long retryMs;

    if (tokens >= permits) {
      tokens -= permits;
      status = ApiProtocol.ST_ALLOWED;
      retryMs = 0;
    } else {
      status = ApiProtocol.ST_DENIED;
      long rateTokens = effectiveRateTokens(s, stepIdx, adaptiveRatePct);
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

    long cap = effectiveCapacity(s, stepIdx, adaptiveRatePct);
    if (tokens > cap) tokens = cap;

    byte status;
    long retryMs;

    if (tokens > 0) {
      tokens -= ONE_PERMIT;
      status = ApiProtocol.ST_ALLOWED;
      retryMs = 0;
    } else {
      status = ApiProtocol.ST_DENIED;
      long rateTokens = effectiveRateTokens(s, stepIdx, adaptiveRatePct);
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
    long now = System.nanoTime();
    boolean replicate = rotateWindowIfNeeded(now);

    adaptiveWindowRequests++;
    if (!success) adaptiveWindowFailures++;

    BucketSpec s = spec();
    int failureRate = failureRatePct();
    if (adaptiveWindowRequests >= s.adaptiveMinRequests() && failureRate >= s.adaptiveFailureRatePct()) {
      int nextPct = Math.max(s.adaptiveMinFactorPct(), adaptiveRatePct - s.adaptiveDecreaseStepPct());
      if (nextPct < adaptiveRatePct) {
        int prev = adaptiveRatePct;
        int req = adaptiveWindowRequests;
        int fail = adaptiveWindowFailures;
        adaptiveRatePct = nextPct;
        adaptiveGoodWindows = 0;
        clampTokensToCurrentCap(s);
        resetWindow(now);
        replicate = true;
        LOG.info("Adaptive rate снижен: bucket={} ratePct {} -> {} failRate={} req={} fail={} warmupStep={}",
            s.name(), prev, nextPct, failureRate, req, fail, stepIdx);
      }
    } else {
      reportsSinceReplica++;
      if (reportsSinceReplica >= REPORT_REPL_EVERY) {
        reportsSinceReplica = 0;
        replicate = true;
      }
    }

    seq++;
    return replicate ? new ReportResult(ApiProtocol.ST_OK, true) : REPORT_NOOP;
  }

  @Override
  public synchronized void applyReplica(BucketState s) {
    if (s == null || s.type() != BucketType.ADAPTIVE_RATE) return;
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
    adaptiveRatePct = s.adaptiveRatePct() <= 0 ? 100 : s.adaptiveRatePct();
    adaptiveWindowStartNanos = s.adaptiveWindowStartNanos();
    adaptiveWindowRequests = s.adaptiveWindowRequests();
    adaptiveWindowFailures = s.adaptiveWindowFailures();
    adaptiveGoodWindows = s.adaptiveGoodWindows();
    reportsSinceReplica = 0;
  }

  @Override
  public synchronized BucketState snapshot() {
    BucketSpec s = spec();
    return new BucketState(
        s.name(), s.version(), seq, BucketType.ADAPTIVE_RATE,
        tokens, lastRefillNanos, lastAccessNanos, stepIdx, stepStartNanos,
        adaptiveRatePct, adaptiveWindowStartNanos, adaptiveWindowRequests, adaptiveWindowFailures, adaptiveGoodWindows
    );
  }

  @Override
  public synchronized long seq() {
    return seq;
  }

  private boolean rotateWindowIfNeeded(long now) {
    long windowNanos = adaptiveWindowNanos;
    if (windowNanos <= 0) return false;
    if (now - adaptiveWindowStartNanos < windowNanos) return false;

    boolean changed = false;
    BucketSpec s = spec();
    if (adaptiveWindowRequests >= s.adaptiveMinRequests()) {
      int failureRate = failureRatePct();
      if (failureRate < s.adaptiveFailureRatePct()) {
        adaptiveGoodWindows++;
        if (adaptiveRatePct < 100 && adaptiveGoodWindows >= GOOD_WINDOWS_TO_INCREASE) {
          int prev = adaptiveRatePct;
          adaptiveRatePct = Math.min(100, adaptiveRatePct + s.adaptiveIncreaseStepPct());
          adaptiveGoodWindows = 0;
          clampTokensToCurrentCap(s);
          changed = adaptiveRatePct != prev;
          if (changed) {
            LOG.info("Adaptive rate восстановлен: bucket={} ratePct {} -> {} failRate={} warmupStep={}",
                s.name(), prev, adaptiveRatePct, failureRate, stepIdx);
          }
        }
      } else {
        adaptiveGoodWindows = 0;
      }
    } else if (adaptiveWindowRequests > 0) {
      adaptiveGoodWindows = 0;
    }

    resetWindow(now);
    return changed;
  }

  private int failureRatePct() {
    if (adaptiveWindowRequests <= 0) return 0;
    return (int) ((adaptiveWindowFailures * 100L) / adaptiveWindowRequests);
  }

  private void resetWindow(long now) {
    adaptiveWindowStartNanos = now;
    adaptiveWindowRequests = 0;
    adaptiveWindowFailures = 0;
    reportsSinceReplica = 0;
  }

  private void clampTokensToCurrentCap(BucketSpec s) {
    long cap = effectiveCapacity(s, stepIdx, adaptiveRatePct);
    if (tokens > cap) tokens = cap;
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

    long cap = effectiveCapacity(s, stepIdx, adaptiveRatePct);
    if (tokens >= cap) {
      tokens = cap;
      lastRefillNanos = now;
      refillRemainder = 0;
      return;
    }

    long rateTokens = effectiveRateTokens(s, stepIdx, adaptiveRatePct);
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
    adaptiveWindowNanos = Math.max(0L, s.adaptiveWindowMs()) * 1_000_000L;
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
      clampTokensToCurrentCap(s);
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
      long fromCap = effectiveCapacity(s, stepIdx, adaptiveRatePct);
      long toCap = effectiveCapacity(s, newIdx, adaptiveRatePct);
      LOG.info("Шаг бакета обновлен: bucket={} step {} -> {} effectiveCap {} -> {} ratePct={}",
          s.name(), stepIdx, newIdx, fromCap, toCap, adaptiveRatePct);
      stepIdx = newIdx;
      stepStartNanos = (newIdx == last) ? now : (stepStartNanos + stepsToAdvance * periodNanos);
      clampTokensToCurrentCap(s);
    }
  }
}
