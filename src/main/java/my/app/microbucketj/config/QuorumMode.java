package my.app.microbucketj.config;

import java.util.Locale;

/**
 * How many replica ACKs to require for a write (token acquire) replication.
 *
 * <p>Goal: run safely in Kubernetes with changing number of pods.
 * Modes based on connectivity avoid "permanent UNAVAILABLE" when cluster is not fully formed.
 */
public enum QuorumMode {
  /**
   * Current behaviour: need = min(MIN_ACKS, replicaSetSize-1). Replica-set is based on membership view.
   */
  STATIC_MIN_ACKS,

  /**
   * need = min(MIN_ACKS, connectedReplicaCount). Only active connections are counted.
   */
  CONNECTED_MIN_ACKS,

  /**
   * Majority of currently connected nodes in replica-set (including self).
   *
   * <p>Example: if connected replicas = 2 (total available = 3 including self), then majority is 2 total,
   * so required replica ACKs = 1.
   */
  CONNECTED_MAJORITY;

  public static QuorumMode fromEnv(String v, QuorumMode def) {
    if (v == null || v.isBlank()) return def;
    String s = v.trim().toUpperCase(Locale.ROOT);
    try {
      return QuorumMode.valueOf(s);
    } catch (IllegalArgumentException ignored) {
      return def;
    }
  }
}
