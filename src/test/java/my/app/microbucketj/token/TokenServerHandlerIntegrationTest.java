package my.app.microbucketj.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.config.ReplicationSettings;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardReportHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenReplicationCodec;
import my.app.microbucketj.replication.ReplicationService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TokenServerHandlerIntegrationTest {

  @Test
  void forwardAcquireHashReturnsAllowedResponse() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("token-bucket", 1, 1, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, new LockRegistry(), replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    try {
      ByteBuf in = Unpooled.buffer(1 + 4 + 8 + 8);
      TokenForwardAcquireHashCodec.writeRequest(in, 201, 1, buckets.get(spec.name()).bucketKey());
      channel.writeInbound(in);

      ForwardResponse response = readForwardResponse(channel);
      assertEquals(TokenProtocol.ST_ALLOWED, response.status());
      assertEquals(201, response.requestId());
      assertEquals(0L, response.remaining());
      assertEquals(0L, response.retryAfterMs());
    } finally {
      closeChannel(channel);
      replication.close();
      tokenPeers.close();
    }
  }

  @Test
  void forwardReportHashOnAdaptiveBucketReturnsOk() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = adaptiveSpec("adaptive-bucket");
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, new LockRegistry(), replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    try {
      ByteBuf in = Unpooled.buffer(1 + 4 + 1 + 8);
      TokenForwardReportHashCodec.writeRequest(in, 202, true, buckets.get(spec.name()).bucketKey());
      channel.writeInbound(in);

      ForwardResponse response = readForwardResponse(channel);
      assertEquals(TokenProtocol.ST_OK, response.status());
      assertEquals(202, response.requestId());
    } finally {
      closeChannel(channel);
      replication.close();
      tokenPeers.close();
    }
  }

  @Test
  void replPutAppliesStateAndReturnsAck() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("replicated-bucket", 1, 10, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, new LockRegistry(), replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    try {
      AbstractBucketRuntime runtime = buckets.get(spec.name());
      BucketState base = runtime.snapshot();
      BucketState state = new BucketState(
          base.bucket(),
          base.specVersion(),
          base.seq() + 7,
          base.type(),
          3,
          base.lastRefillNanos(),
          base.lastAccessNanos(),
          base.stepIndex(),
          base.stepStartNanos(),
          0,
          0L,
          0,
          0,
          0
      );

      ByteBuf in = Unpooled.buffer();
      TokenReplicationCodec.writeReplPut(in, 303, 1, state);
      channel.writeInbound(in);

      AckResponse ack = readAckResponse(channel);
      assertEquals(303, ack.corrId());
      assertEquals(true, ack.ok());

      BucketState applied = runtime.snapshot();
      assertEquals(state.seq(), applied.seq());
      assertEquals(state.tokens(), applied.tokens());
    } finally {
      closeChannel(channel);
      replication.close();
      tokenPeers.close();
    }
  }

  @Test
  void stateTransferAppliesStateWithoutAck() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketSpec spec = new BucketSpec("state-xfer-bucket", 1, 10, 1, 3_600_000);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, new LockRegistry(), replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    try {
      AbstractBucketRuntime runtime = buckets.get(spec.name());
      BucketState base = runtime.snapshot();
      BucketState state = new BucketState(
          base.bucket(),
          base.specVersion(),
          base.seq() + 3,
          base.type(),
          4,
          base.lastRefillNanos(),
          base.lastAccessNanos(),
          base.stepIndex(),
          base.stepStartNanos(),
          0,
          0L,
          0,
          0,
          0
      );

      ByteBuf in = Unpooled.buffer();
      TokenReplicationCodec.writeStateXfer(in, 1, state);
      channel.writeInbound(in);

      assertNull(channel.readOutbound());
      BucketState applied = runtime.snapshot();
      assertEquals(state.seq(), applied.seq());
      assertEquals(state.tokens(), applied.tokens());
    } finally {
      closeChannel(channel);
      replication.close();
      tokenPeers.close();
    }
  }

  private static TokenPeerClient newTokenPeers() {
    return new TokenPeerClient(1, 65_536, 1_000, 3_000, "n1", 1_000, 100);
  }

  private static ReplicationService newDisabledReplication(MembershipRuntime membership, TokenPeerClient tokenPeers) {
    return new ReplicationService(
        membership,
        tokenPeers,
        new ReplicationSettings(QuorumMode.CONNECTED_MAJORITY, 0, false, ReplicationMode.DISABLED, 25, 100)
    );
  }

  private static BucketSpec adaptiveSpec(String name) {
    return new BucketSpec(
        name,
        1,
        BucketType.ADAPTIVE_RATE,
        10,
        1,
        1_000,
        null,
        0,
        0,
        1_000,
        4,
        50,
        20,
        10,
        10
    );
  }

  private static ForwardResponse readForwardResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      assertEquals(TokenProtocol.FWD_RESP_PAYLOAD_LEN, out.readInt());
      assertEquals(TokenProtocol.OP_FWD_RESP, out.readByte());
      ForwardResponse response = new ForwardResponse(
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

  private static AckResponse readAckResponse(EmbeddedChannel channel) {
    ByteBuf out = channel.readOutbound();
    assertNotNull(out);
    try {
      assertEquals(TokenProtocol.REPL_ACK_PAYLOAD_LEN, out.readInt());
      assertEquals(TokenProtocol.OP_REPL_ACK, out.readByte());
      AckResponse response = new AckResponse(out.readInt(), out.readByte() == 1);
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

  private record ForwardResponse(byte status, int requestId, long remaining, long retryAfterMs) {
  }

  private record AckResponse(int corrId, boolean ok) {
  }

  // ---- ForwardLockAcquireOp ----
  @Test
  void forwardLockAcquire_applied() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of());
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, new LockRegistry(), replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    try {
      ByteBuf in = Unpooled.buffer(128);
      TokenForwardLockCodec.writeLockAcquireRequest(in, 701, "job-1",
          System.currentTimeMillis() + 60_000, "node-A");
      channel.writeInbound(in);

      ByteBuf out = channel.readOutbound();
      assertNotNull(out);
      try {
        out.readInt();                                    // payload size
        byte op = out.readByte();
        assertEquals(InternalProtocol.OP_LOCK_FWD_RESP /* или OP_LOCK_FWD_RESP, если используем единый */, op);
        var resp = TokenForwardLockCodec.readForwardResponse(out);
        assertEquals(ApiProtocol.ST_OK, resp.status());
        assertEquals(701, resp.requestId());
      } finally {
        out.release();
      }
    } finally {
      closeChannel(channel);
      replication.close();
      tokenPeers.close();
    }
  }

  // ---- ForwardLockDeleteOp ----
  @Test
  void forwardLockDelete_removesEntry() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of());
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry locks = new LockRegistry();
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, locks, replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    ByteBuf in = Unpooled.buffer(96);
    TokenForwardLockCodec.writeLockDeleteRequest(in, 702, "job-1");
    channel.writeInbound(in);
    assertNull(locks.get("job-1"));
  }

  // ---- ReplLockOp ----
  @Test
  void replLock_appliesRemoteEntry() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of());
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry locks = new LockRegistry();
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, locks, replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    LockEntry entry = new LockEntry("job-1", 1_000_000L, 900_000L, "node-A");
    ByteBuf in = Unpooled.buffer(256);
    TokenForwardLockCodec.writeLockReplicationRequest(in, 301, 1, 0xDEADBEEFL, "job-1", entry, false);
    channel.writeInbound(in);

    assertEquals("node-A", locks.get("job-1").lockedBy());
    // проверить, что ack ушёл:
    ByteBuf ack = channel.readOutbound();
    assertNotNull(ack);
    ack.release();
  }

  @Test
  void replLock_deleteRemovesEntry() {
    Node self = new Node("n1", "127.0.0.1", 9001);
    BucketRegistry buckets = new BucketRegistry(java.util.Map.of());
    MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
    TokenPeerClient tokenPeers = newTokenPeers();
    ReplicationService replication = newDisabledReplication(membership, tokenPeers);
    LockRegistry locks = new LockRegistry();
    LockExecutor lockExecutor = new LockExecutor(() -> true, membership, locks, replication);
    EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(lockExecutor, buckets, replication, membership));
    ByteBuf in = Unpooled.buffer(128);
    TokenForwardLockCodec.writeLockReplicationRequest(in, 302, 1, 0xDEADBEEFL, "job-1", null, true);
    channel.writeInbound(in);
    assertNull(locks.get("job-1"));
  }
}
