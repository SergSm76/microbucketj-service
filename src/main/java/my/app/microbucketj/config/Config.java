package my.app.microbucketj.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.model.HostAndPort;
import my.app.microbucketj.cluster.model.Node;

import java.util.Arrays;
import java.util.Map;

public final class Config {

  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  public final String nodeId;

  public final String bindHost;
  public final String advertiseHost;

  public final int publicPort;
  public final int clusterPort;

  public final String discoveryDns;
  public final HostAndPort[] discoveryHosts;

  public final int ioThreads;
  public final int publicMaxFrame;
  public final int internalMaxFrame;

  public final int replicaCount;
  public final InternalTimeouts timeouts;
  public final ReplicationSettings replication;

  public final long reconnectPeriodMs;
  public final long heartbeatIntervalMs;
  public final long heartbeatTimeoutMs;
  public final long leaderHoldDownMs;

  public final BucketRegistry bucketRegistry;

  public final boolean observabilityEnabled;
  public final int observabilityPort;
  public final boolean locksObservabilityApiEnabled;

  public final long locksCleanupPeriodMinutes;

  public final SslConfig sslConfig;

  private Config(String nodeId,
                 String bindHost,
                 String advertiseHost,
                 int publicPort,
                 int clusterPort,
                 String discoveryDns,
                 HostAndPort[] discoveryHosts,
                 int ioThreads,
                 int publicMaxFrame,
                 int internalMaxFrame,
                 int replicaCount,
                 InternalTimeouts timeouts,
                 ReplicationSettings replication,
                 long reconnectPeriodMs,
                 long heartbeatIntervalMs,
                 long heartbeatTimeoutMs,
                 long leaderHoldDownMs,
                 BucketRegistry bucketRegistry,
                 boolean observabilityEnabled,
                 int observabilityPort,
                 boolean locksObservabilityApiEnabled,
                 long locksCleanupPeriodMinutes,
                 SslConfig sslConfig) {
    this.nodeId = nodeId;
    this.bindHost = bindHost;
    this.advertiseHost = advertiseHost;
    this.publicPort = publicPort;
    this.clusterPort = clusterPort;
    this.discoveryDns = discoveryDns;
    this.discoveryHosts = discoveryHosts;
    this.ioThreads = ioThreads;
    this.publicMaxFrame = publicMaxFrame;
    this.internalMaxFrame = internalMaxFrame;
    this.replicaCount = replicaCount;
    this.timeouts = timeouts;
    this.replication = replication;
    this.reconnectPeriodMs = reconnectPeriodMs;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.leaderHoldDownMs = leaderHoldDownMs;
    this.bucketRegistry = bucketRegistry;
    this.observabilityEnabled = observabilityEnabled;
    this.observabilityPort = observabilityPort;
    this.locksObservabilityApiEnabled = locksObservabilityApiEnabled;
    this.locksCleanupPeriodMinutes = locksCleanupPeriodMinutes;
    this.sslConfig = sslConfig;
  }

  public static Config fromEnv() {
    String nodeId = Env.getEnv("POD_NAME", "n1");

    String bindHost = Env.getEnv("BIND_HOST", "0.0.0.0");
    String advertiseHost = Env.getEnv("HOST_IP", "127.0.0.1");

    int publicPort = Env.getEnvInteger("PUBLIC_PORT", 12005);
    int clusterPort = Env.getEnvInteger("CLUSTER_PORT", 9001);

    String discoveryDns = Env.getEnv("DISCOVERY_DNS", Env.getEnv("SERVICE-NAME", ""));
    String discoveryHostsRaw = Env.getEnv("DISCOVERY_HOSTS", "");
    HostAndPort[] discoveryHosts = splitHosts(discoveryHostsRaw, clusterPort);

    int ioThreads = Env.getEnvInteger("IO_THREADS", defaultIoThreads());
    int publicMaxFrame = Env.getEnvInteger("PUBLIC_MAX_FRAME", 4096);
    int internalMaxFrame = Env.getEnvInteger("INTERNAL_MAX_FRAME", 65536);

    int replicaCount = Env.getEnvInteger("REPLICA_COUNT", 1);
    InternalTimeouts timeouts = InternalTimeouts.fromEnv(5);
    ReplicationSettings replication = ReplicationSettings.fromEnv(timeouts);

    long reconnectPeriodMs = Math.max(100, Env.getEnvLong("RECONNECT_PERIOD_MS", 1000));
    long heartbeatIntervalMs = Math.max(100, Env.getEnvLong("HEARTBEAT_INTERVAL_MS", 1000));
    long heartbeatTimeoutMs = Env.getEnvLong("HEARTBEAT_TIMEOUT_MS", Math.max(heartbeatIntervalMs * 3, 3000));
    if (heartbeatTimeoutMs < heartbeatIntervalMs * 2) {
      heartbeatTimeoutMs = heartbeatIntervalMs * 2;
    }

    long leaderHoldDownMs = Math.max(0, Env.getEnvLong("LEADER_HOLD_DOWN_MS", 500));

    String bucketsRaw = Env.getEnv("RLS_BUCKETS", "");
    Map<String, BucketSpec> bucketSpecs = BucketSpecParser.parseBuckets(bucketsRaw);
    BucketRegistry bucketRegistry = new BucketRegistry(bucketSpecs);

    boolean observabilityEnabled = Env.getEnvBoolean("OBSERVABILITY_ENABLED", true);
    int observabilityPort = Env.getEnvInteger("OBSERVABILITY_PORT", 8083);
    boolean locksObservabilityApiEnabled = Env.getEnvBoolean("LOCKS_OBSERVABILITY_API_ENABLED", true);

    long locksCleanupPeriodMinutes = Math.min(100, Env.getEnvLong("LOCKS_CLEANUP_PERIOD_MINUTES", 10));

    SslConfig sslConfig = SslConfig.fromEnv();

    validate(nodeId, bindHost, advertiseHost,
        publicPort, clusterPort,
        replicaCount, replication.minAcks(), replication.ackTimeoutMs(),
        publicMaxFrame, internalMaxFrame,
        heartbeatIntervalMs, heartbeatTimeoutMs,
        leaderHoldDownMs);

    return new Config(nodeId, bindHost, advertiseHost,
        publicPort, clusterPort,
        discoveryDns, discoveryHosts,
        ioThreads, publicMaxFrame, internalMaxFrame,
        replicaCount,
        timeouts,
        replication,
        reconnectPeriodMs,
        heartbeatIntervalMs, heartbeatTimeoutMs,
        leaderHoldDownMs,
        bucketRegistry,
        observabilityEnabled,
        observabilityPort,
        locksObservabilityApiEnabled,
        locksCleanupPeriodMinutes,
        sslConfig
    );
  }

  private static void validate(String nodeId,
                               String bindHost,
                               String advertiseHost,
                               int publicPort,
                               int clusterPort,
                               int replicaCount,
                               int minAcks,
                               long ackTimeoutMs,
                               int publicMaxFrame,
                               int internalMaxFrame,
                               long heartbeatIntervalMs,
                               long heartbeatTimeoutMs,
                               long leaderHoldDownMs) {
    if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("NODE_ID is blank");
    if (bindHost == null || bindHost.isBlank()) throw new IllegalArgumentException("BIND_HOST is blank");
    if (advertiseHost == null || advertiseHost.isBlank()) {
      throw new IllegalArgumentException("ADVERTISE_HOST is blank");
    }

    if (isNotPort(publicPort)) throw new IllegalArgumentException("PUBLIC_PORT invalid: " + publicPort);
    if (isNotPort(clusterPort)) throw new IllegalArgumentException("CLUSTER_PORT invalid: " + clusterPort);

    if (replicaCount <= 0) throw new IllegalArgumentException("REPLICA_COUNT must be > 0");
    if (minAcks < 0) throw new IllegalArgumentException("MIN_ACKS must be >= 0");
    if (ackTimeoutMs <= 0) throw new IllegalArgumentException("ACK_TIMEOUT_MS must be > 0");

    if (publicMaxFrame < 256) throw new IllegalArgumentException("PUBLIC_MAX_FRAME too small: " + publicMaxFrame);
    if (internalMaxFrame < 1024) {
      throw new IllegalArgumentException("INTERNAL_MAX_FRAME too small: " + internalMaxFrame);
    }

    if (heartbeatIntervalMs <= 0) throw new IllegalArgumentException("HEARTBEAT_INTERVAL_MS must be > 0");
    if (heartbeatTimeoutMs <= 0) throw new IllegalArgumentException("HEARTBEAT_TIMEOUT_MS must be > 0");
    if (heartbeatTimeoutMs < heartbeatIntervalMs) {
      throw new IllegalArgumentException("HEARTBEAT_TIMEOUT_MS must be >= HEARTBEAT_INTERVAL_MS");
    }

    if (leaderHoldDownMs < 0) {
      throw new IllegalArgumentException("LEADER_HOLD_DOWN_MS must be >= 0");
    }
  }

  private static boolean isNotPort(int port) {
    return port < 1 || port > 65535;
  }

  private static int defaultIoThreads() {
    int cpus = Runtime.getRuntime().availableProcessors();
    return Math.max(2, Math.min(4, cpus));
  }

  private static HostAndPort[] splitHosts(String raw, int clusterPort) {
    if (raw == null) return new HostAndPort[0];
    String s = raw.trim();
    if (s.isEmpty()) return new HostAndPort[0];
    String[] split = s.replace(';', ',')
        .replace(' ', ',')
        .split(",+");
    HostAndPort[] hostAndPorts = new HostAndPort[split.length];
    for (int i = 0; i < split.length; i++) {
      String string = split[i];
      if (string.contains(":")) {
        String[] parts = string.split(":");
        HostAndPort hostAndPort = new HostAndPort(parts[0], Integer.parseInt(parts[1]));
        hostAndPorts[i] = hostAndPort;
      } else {
        HostAndPort hostAndPort = new HostAndPort(string, clusterPort);
        hostAndPorts[i] = hostAndPort;
      }
    }
    return hostAndPorts;
  }

  public Node selfNode() {
    return new Node(nodeId, advertiseHost, clusterPort);
  }

  public void logSummary() {
    LOG.info("nodeId={} bindHost={} advertiseHost={} publicPort={} clusterPort={}",
        nodeId,
        bindHost,
        advertiseHost,
        publicPort,
        clusterPort);

    if (discoveryDns != null && !discoveryDns.isEmpty()) {
      LOG.info("discoveryDns={}", discoveryDns);
    } else {
      LOG.info("discoveryHosts={}", Arrays.toString(discoveryHosts));
    }

    LOG.info("Replication: replicaCount={} minAcks={} ackTimeoutMs={} strictAcks={} quorumMode={} replicationMode={} replBatchMs={}",
        replicaCount,
        replication.minAcks(),
        replication.ackTimeoutMs(),
        replication.strictAcks(),
        replication.quorumMode(),
        replication.mode(),
        replication.batchMs()
    );

    LOG.info(
        "RLS config: forwardTimeoutMs={} reconnectPeriodMs={} heartbeatIntervalMs={} heartbeatTimeoutMs={} leaderHoldDownMs={} initBuckets={}",
        timeouts.forwardTimeoutMs(),
        reconnectPeriodMs,
        heartbeatIntervalMs,
        heartbeatTimeoutMs,
        leaderHoldDownMs,
        bucketRegistry.bucketNames()
    );
  }
}
