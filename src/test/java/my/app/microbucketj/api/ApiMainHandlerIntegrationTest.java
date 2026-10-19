package my.app.microbucketj.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.config.ReplicationSettings;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import my.app.microbucketj.utils.Hashing;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

// TODO рефакторинг
class ApiMainHandlerIntegrationTest {

  @Test
  void acquireHashReturnsAllowedResponse() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("api-bucket", 1, 2, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      long bucketHash = buckets.get(spec.name()).bucketKey();
      channel.writeInbound(apiAcquireHashRequest(101, 1, bucketHash));

      AcquireResponse response = readAcquireResponse(channel);
      assertEquals(ApiProtocol.ST_ALLOWED, response.status());
      assertEquals(101, response.requestId());
      assertEquals(1L, response.remaining());
      assertEquals(0L, response.retryAfterMs());
      assertNull(channel.readOutbound());
    } finally {
      closeChannel(channel);
      replication.close();
      clusterPeers.close();
      tokenPeers.close();
    }
  }

  @Test
  void acquireHashMissingBucketReturnsNotFound() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of());
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      byte[] missing = "missing".getBytes(UTF_8);
      long bucketHash = Hashing.fnv1a64(missing, 0, missing.length);
      channel.writeInbound(apiAcquireHashRequest(102, 1, bucketHash));

      AcquireResponse response = readAcquireResponse(channel);
      assertEquals(ApiProtocol.ST_NOT_FOUND, response.status());
      assertEquals(102, response.requestId());
      assertEquals(0L, response.remaining());
      assertEquals(0L, response.retryAfterMs());
    } finally {
      closeChannel(channel);
      replication.close();
      clusterPeers.close();
      tokenPeers.close();
    }
  }

  @Test
  void reportHashOnTokenBucketReturnsOk() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("report-bucket", 1, 5, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      long bucketHash = buckets.get(spec.name()).bucketKey();
      channel.writeInbound(apiReportHashRequest(103, true, bucketHash));

      ReportResponse response = readReportResponse(channel);
      assertEquals(ApiProtocol.ST_OK, response.status());
      assertEquals(103, response.requestId());
    } finally {
      closeChannel(channel);
      replication.close();
      clusterPeers.close();
      tokenPeers.close();
    }
  }

  private static TokenPeerClient newTokenPeers() {
    return new TokenPeerClient(1, 65_536, 1_000, 3_000, "n1", 1_000, 100);
  }

  private static ClusterPeerClient newClusterPeers() {
    return new ClusterPeerClient(1, 65_536, 1_000, 3_000, "n1", 1_000);
  }

  private static ReplicationService newDisabledReplication(MembershipRuntime membership, TokenPeerClient tokenPeers) {
    return new ReplicationService(
        membership,
        tokenPeers,
        new ReplicationSettings(QuorumMode.CONNECTED_MAJORITY, 0, false, ReplicationMode.DISABLED, 25, 100)
    );
  }

  private static ByteBuf apiAcquireHashRequest(int requestId, long permits, long bucketHash) {
    ByteBuf in = Unpooled.buffer(1 + 4 + 8 + 8);
    in.writeByte(ApiProtocol.OP_ACQUIRE_HASH);
    in.writeInt(requestId);
    in.writeLong(permits);
    in.writeLong(bucketHash);
    return in;
  }

  private static ByteBuf apiReportHashRequest(int requestId, boolean success, long bucketHash) {
    ByteBuf in = Unpooled.buffer(1 + 4 + 1 + 8);
    in.writeByte(ApiProtocol.OP_REPORT_HASH);
    in.writeInt(requestId);
    in.writeByte(success ? 1 : 0);
    in.writeLong(bucketHash);
    return in;
  }

  private static AcquireResponse readAcquireResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      assertEquals(ApiProtocol.RESP_ACQUIRE_SIZE, out.readInt());
      AcquireResponse response = new AcquireResponse(
          out.readByte(),
          out.readInt(),
          out.readLong(),
          out.readLong()
      );
      assertEquals(0, out.readableBytes());
      return response;
    } finally {
      out.release();
    }
  }

  private static ReportResponse readReportResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      assertEquals(ApiProtocol.RESP_REPORT_SIZE, out.readInt());
      ReportResponse response = new ReportResponse(out.readByte(), out.readInt());
      assertEquals(0, out.readableBytes());
      return response;
    } finally {
      out.release();
    }
  }

  private static void closeChannel(EmbeddedChannel channel) {
    if (channel != null) {
      channel.finishAndReleaseAll();
    }
  }

  private record AcquireResponse(byte status, int requestId, long remaining, long retryAfterMs) {
  }

  private record ReportResponse(byte status, int requestId) {
  }

  // Хелпер — формировать запрос LOCK_ACQUIRE:
  private static ByteBuf apiLockAcquireRequest(int requestId, String name, long lockUntilMs, String lockedBy) {
    byte[] nameB = name.getBytes(UTF_8);
    byte[] ownerB = lockedBy.getBytes(UTF_8);
    ByteBuf in = Unpooled.buffer(1 + 4 + 1 + nameB.length + 8 + 1 + ownerB.length);
    in.writeByte(ApiProtocol.OP_LOCK_ACQUIRE);
    in.writeInt(requestId);
    in.writeByte(nameB.length);
    in.writeBytes(nameB);
    in.writeLong(lockUntilMs);
    in.writeByte(ownerB.length);
    in.writeBytes(ownerB);
    return in;
  }

  private static ByteBuf apiLockGetRequest(int requestId, String name) {
    byte[] nameB = name.getBytes(UTF_8);
    ByteBuf in = Unpooled.buffer(1 + 4 + 1 + nameB.length);
    in.writeByte(ApiProtocol.OP_LOCK_GET);
    in.writeInt(requestId);
    in.writeByte(nameB.length);
    in.writeBytes(nameB);
    return in;
  }

  // Хелпер — прочитать lock-ответ (acquire/update/release):
  private static LockResponse readLockResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      assertEquals(ApiProtocol.RESP_LOCK_SIZE, out.readInt());
      LockResponse r = new LockResponse(out.readByte(), out.readInt(), out.readLong(), out.readLong());
      assertEquals(0, out.readableBytes());
      return r;
    } finally {
      out.release();
    }
  }

  // Хелпер — прочитать lock-get-ответ (с lockedBy):
  private static LockGetResponse readLockGetResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      out.readInt();                                  // payload size
      byte status = out.readByte();
      int reqId = out.readInt();
      long lockUntil = out.readLong();
      long lockedAt = out.readLong();
      int len = out.readUnsignedByte();
      byte[] owner = new byte[len];
      out.readBytes(owner);
      assertEquals(0, out.readableBytes());
      return new LockGetResponse(status, reqId, lockUntil, lockedAt,
          new String(owner, UTF_8));
    } finally {
      out.release();
    }
  }

  private record LockResponse(byte status, int requestId, long lockUntil, long lockedAt) {
  }

  private record LockGetResponse(byte status, int requestId, long lockUntil, long lockedAt, String lockedBy) {
  }

  // ---- тесты ----
  @Test
  void lockAcquire_applied() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("api-bucket", 1, 2, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      channel.writeInbound(apiLockAcquireRequest(501, "job-1", System.currentTimeMillis() + 60_000, "node-A"));
      var r = readLockResponse(channel);
      assertEquals(ApiProtocol.ST_OK, r.status());
      assertEquals(501, r.requestId());
    } finally {
      closeChannel(channel); /*...*/
    }
  }

  @Test
  void lockAcquire_heldByOther() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("api-bucket", 1, 2, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    lockRegistry.acquire("job-1", System.currentTimeMillis() + 60_000, "node-A");
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      channel.writeInbound(apiLockAcquireRequest(502, "job-1", System.currentTimeMillis() + 60_000, "node-B"));
      var r = readLockResponse(channel);
      assertEquals(ApiProtocol.ST_LOCK_HELD, r.status());
    } finally {
      closeChannel(channel); /*...*/
    }
  }

  @Test
  void lockGet_onEmptyReturnsNotFound() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("api-bucket", 1, 2, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      channel.writeInbound(apiLockGetRequest(503, "missing"));
      var r = readLockGetResponse(channel);
      assertEquals(ApiProtocol.ST_NOT_FOUND, r.status());
      assertEquals(503, r.requestId());
    } finally {
      closeChannel(channel); /*...*/
    }
  }

  @Test
  void lockGet_returnsHeldEntry() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("api-bucket", 1, 2, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ClusterPeerClient clusterPeers = newClusterPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry lockRegistry = new LockRegistry();
    lockRegistry.acquire("job-1", System.currentTimeMillis() + 60_000, "node-A");
    EmbeddedChannel channel = new EmbeddedChannel(
        new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication, lockRegistry));
    try {
      channel.writeInbound(apiLockGetRequest(504, "job-1"));
      var r = readLockGetResponse(channel);
      assertEquals(ApiProtocol.ST_OK, r.status());
      assertEquals("node-A", r.lockedBy());
    } finally {
      closeChannel(channel); /*...*/
    }
  }
}
