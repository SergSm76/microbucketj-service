package my.app.microbucketj.bucket;

import my.app.microbucketj.config.BucketType;

/**
 * Replicated state for both plain and adaptive rate limiters.
 */
public record BucketState(
    String bucket,
    long specVersion,
    long seq,
    BucketType type,

    long tokens,
    long lastRefillNanos,
    long lastAccessNanos,
    int stepIndex,
    long stepStartNanos,

    int adaptiveRatePct,
    long adaptiveWindowStartNanos,
    int adaptiveWindowRequests,
    int adaptiveWindowFailures,
    int adaptiveGoodWindows
) {
}
