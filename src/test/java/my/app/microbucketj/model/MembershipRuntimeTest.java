package my.app.microbucketj.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;

class MembershipRuntimeTest {

    @Test
    void ignoresStaleEpochUpdates() {
        Node n1 = new Node("n1", "127.0.0.1", 9001);
        Node n2 = new Node("n2", "127.0.0.1", 9002);
        Node n3 = new Node("n3", "127.0.0.1", 9003);
        MembershipRuntime membership = new MembershipRuntime("n1", 3, 5, List.of(n1, n2));

        membership.applyMembership(4, List.of(n1, n2, n3));

        MembershipRuntime.Snapshot snap = membership.snapshot();
        assertEquals(5, snap.epoch());
        assertIterableEquals(List.of(n1, n2), snap.nodes());
    }

    @Test
    void appliesNewerEpochUpdates() {
        Node n1 = new Node("n1", "127.0.0.1", 9001);
        Node n2 = new Node("n2", "127.0.0.1", 9002);
        Node n3 = new Node("n3", "127.0.0.1", 9003);
        MembershipRuntime membership = new MembershipRuntime("n1", 3, 1, List.of(n1, n2));

        membership.applyMembership(2, List.of(n1, n2, n3));

        MembershipRuntime.Snapshot snap = membership.snapshot();
        assertEquals(2, snap.epoch());
        assertIterableEquals(List.of(n1, n2, n3), snap.nodes());
        assertEquals("n1", snap.view().self().id());
    }
}
