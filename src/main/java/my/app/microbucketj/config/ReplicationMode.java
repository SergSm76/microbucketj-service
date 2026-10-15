package my.app.microbucketj.config;

/**
 * Replication strategy.
 *
 * DISABLED     - do not replicate primary mutations or state handoff.
 * PER_REQUEST  - replicate each primary mutation and (optionally) wait for quorum.
 * BATCH        - coalesce primary mutations and replicate latest state periodically.
 */
public enum ReplicationMode {

  DISABLED,
  PER_REQUEST,
  BATCH;

  public static ReplicationMode fromEnv(String raw, ReplicationMode def) {
    if (raw == null || raw.isBlank()) return def;
    String v = raw.trim().toUpperCase();
    return switch (v) {
      case "DISABLED", "DISABLE", "OFF", "NONE", "NO" -> DISABLED;
      case "PER_REQUEST", "PER", "REQUEST" -> PER_REQUEST;
      case "BATCH", "BATCHED" -> BATCH;
      default -> def;
    };
  }
}
