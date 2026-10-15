package my.app.microbucketj.cluster.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.utils.Hashing;

class MembershipOpTest {

    @Test
    void stateTransferTargetsIncludeNewPrimaryWhenScaleOutMovesBucket() {
        Node n1 = new Node("rls-0", "10.0.0.1", 9001);
        Node n2 = new Node("rls-1", "10.0.0.2", 9001);

        ClusterView oldView = new ClusterView("rls-0", List.of(n1), 3);
        ClusterView newView = new ClusterView("rls-0", List.of(n1, n2), 3);

        long bucketHash = findBucketHash(newView, "rls-1");

        assertEquals("rls-0", oldView.pickPrimary(bucketHash).id());
        assertEquals("rls-1", newView.pickPrimary(bucketHash).id());
        assertIterableEquals(List.of(n2), MembershipOp.stateTransferTargets(oldView, newView, bucketHash));
    }

    @Test
    void stateTransferTargetsAreEmptyWhenSelfWasNotPreviousPrimary() {
        Node n1 = new Node("rls-0", "10.0.0.1", 9001);
        Node n2 = new Node("rls-1", "10.0.0.2", 9001);
        Node n3 = new Node("rls-2", "10.0.0.3", 9001);

        ClusterView oldView = new ClusterView("rls-0", List.of(n1, n2), 3);
        ClusterView newView = new ClusterView("rls-0", List.of(n1, n2, n3), 3);

        long bucketHash = findBucketHash(oldView, "rls-1");

        assertEquals("rls-1", oldView.pickPrimary(bucketHash).id());
        assertTrue(MembershipOp.stateTransferTargets(oldView, newView, bucketHash).isEmpty());
    }

    private static long findBucketHash(ClusterView view, String primaryId) {
        for (int i = 0; i < 10_000; i++) {
            String bucket = "bucket-" + i;
            byte[] bytes = bucket.getBytes(StandardCharsets.UTF_8);
            long hash = Hashing.fnv1a64(bytes, 0, bytes.length);
            if (primaryId.equals(view.pickPrimary(hash).id())) {
                return hash;
            }
        }
        throw new AssertionError("Unable to find bucket hash for primary " + primaryId);
    }
}
