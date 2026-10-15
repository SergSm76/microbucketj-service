package my.app.microbucketj.token;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.config.ReplicationSettings;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardReportHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenReplicationCodec;
import my.app.microbucketj.replication.ReplicationService;


class TokenServerHandlerIntegrationTest {

    @Test
    void forwardAcquireHashReturnsAllowedResponse() {
        Node self = new Node("n1", "127.0.0.1", 9001);
        BucketSpec spec = new BucketSpec("token-bucket", 1, 1, 1, 3_600_000);
        BucketRegistry buckets = new BucketRegistry(java.util.Map.of(spec.name(), spec));
        MembershipRuntime membership = new MembershipRuntime(self.id(), 1, 1, List.of(self));
        TokenPeerClient tokenPeers = newTokenPeers();
        ReplicationService replication = newDisabledReplication(membership, tokenPeers);
        EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(membership, buckets, replication));
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
        EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(membership, buckets, replication));
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
        EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(membership, buckets, replication));
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
        EmbeddedChannel channel = new EmbeddedChannel(new TokenServerHandler(membership, buckets, replication));
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

    private record ForwardResponse(byte status, int requestId, long remaining, long retryAfterMs) {}
    private record AckResponse(int corrId, boolean ok) {}
}
