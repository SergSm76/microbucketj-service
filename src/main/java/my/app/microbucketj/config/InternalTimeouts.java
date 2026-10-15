package my.app.microbucketj.config;

/**
 * Internal RPC timeouts.
 *
 * Kept separate from replication to avoid leaking replication settings into unrelated code.
 */
public record InternalTimeouts(long forwardTimeoutMs, long replicationAckTimeoutMs) {

  public InternalTimeouts {
    if (forwardTimeoutMs <= 0) throw new IllegalArgumentException("forwardTimeoutMs must be > 0");
    if (replicationAckTimeoutMs <= 0) throw new IllegalArgumentException("replicationAckTimeoutMs must be > 0");
  }

  public static InternalTimeouts fromEnv(long defaultAckTimeoutMs) {
    long ack = Math.max(1, Env.getEnvLong("ACK_TIMEOUT_MS", defaultAckTimeoutMs));
    long fwd = Math.max(1, Env.getEnvLong("FORWARD_TIMEOUT_MS", ack));
    return new InternalTimeouts(fwd, ack);
  }
}
