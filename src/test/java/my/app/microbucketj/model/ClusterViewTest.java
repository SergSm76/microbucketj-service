package my.app.microbucketj.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.utils.Hashing;

class ClusterViewTest {

    @Test
    void pickReplicasIncludesPrimaryWithoutDuplicates() {
        List<Node> nodes = List.of(
            new Node("n1", "127.0.0.1", 9001),
            new Node("n2", "127.0.0.1", 9002),
            new Node("n3", "127.0.0.1", 9003),
            new Node("n4", "127.0.0.1", 9004)
        );
        ClusterView view = new ClusterView("n1", nodes, 3);
        byte[] key = "bucket".getBytes(StandardCharsets.UTF_8);
        long keyHash = Hashing.fnv1a64(key, 0, key.length);

        Node primary = view.pickPrimary(keyHash);
        List<Node> replicas = view.pickReplicas(keyHash);
        Set<String> ids = replicas.stream().map(Node::id).collect(Collectors.toSet());

        assertEquals(3, replicas.size());
        assertEquals(replicas.size(), ids.size());
        assertTrue(ids.contains(primary.id()));
    }

    @Test
    void pickReplicasIsBoundedByNodeCount() {
        List<Node> nodes = List.of(
            new Node("n1", "127.0.0.1", 9001),
            new Node("n2", "127.0.0.1", 9002)
        );
        ClusterView view = new ClusterView("n1", nodes, 5);
        byte[] key = "tiny".getBytes(StandardCharsets.UTF_8);
        long keyHash = Hashing.fnv1a64(key, 0, key.length);

        List<Node> replicas = view.pickReplicas(keyHash);

        assertEquals(2, replicas.size());
    }
}
