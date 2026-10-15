package my.app.microbucketj.config;

/**
 * Replication parameters grouped into a single value.
 */
public record ReplicationSettings(
    QuorumMode quorumMode,
    int minAcks,
    boolean strictAcks,
    ReplicationMode mode,
    long batchMs,
    long ackTimeoutMs
) {
  public ReplicationSettings {
    if (minAcks < 0) throw new IllegalArgumentException("minAcks must be >= 0");
    if (batchMs <= 0) throw new IllegalArgumentException("batchMs must be > 0");
    if (ackTimeoutMs <= 0) throw new IllegalArgumentException("ackTimeoutMs must be > 0");
    quorumMode = (quorumMode == null) ? QuorumMode.CONNECTED_MAJORITY : quorumMode;
    mode = (mode == null) ? ReplicationMode.BATCH : mode;
  }

  public static ReplicationSettings fromEnv(InternalTimeouts timeouts) {
    int minAcks = Env.getEnvInteger("MIN_ACKS", 1);
    boolean strictAcks = Env.getEnvBoolean("STRICT_ACKS", false);
    QuorumMode quorumMode = QuorumMode.fromEnv(System.getenv("QUORUM_MODE"), QuorumMode.CONNECTED_MAJORITY);

    ReplicationMode mode = ReplicationMode.fromEnv(System.getenv("REPLICATION_MODE"), ReplicationMode.BATCH);
    long batchMs = Math.max(1, Env.getEnvLong("REPL_BATCH_MS", 25));

    return new ReplicationSettings(quorumMode, minAcks, strictAcks, mode, batchMs, timeouts.replicationAckTimeoutMs());
  }
}
