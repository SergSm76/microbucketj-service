package my.app.microbucketj.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.QuorumMode;
import my.app.microbucketj.config.ReplicationMode;
import my.app.microbucketj.config.ReplicationSettings;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import my.app.microbucketj.utils.Hashing;

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
        EmbeddedChannel channel = new EmbeddedChannel(new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication));
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
        EmbeddedChannel channel = new EmbeddedChannel(new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication));
        try {
            byte[] missing = "missing".getBytes(StandardCharsets.UTF_8);
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
        EmbeddedChannel channel = new EmbeddedChannel(new ApiMainHandler(() -> true, membership, buckets, tokenPeers, clusterPeers, replication));
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

    private record AcquireResponse(byte status, int requestId, long remaining, long retryAfterMs) {}
    private record ReportResponse(byte status, int requestId) {}
}
