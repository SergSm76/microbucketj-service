package my.app.microbucketj.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;

class BucketRegistryTest {

    @Test
    void upsertNewerSameTypeUpdatesRuntimeInPlace() {
        BucketSpec initial = new BucketSpec("api", 1, 10, 1, 1_000);
        BucketRegistry registry = new BucketRegistry(Map.of(initial.name(), initial));

        AbstractBucketRuntime before = registry.get("api");
        BucketRegistry.UpsertResult result = registry.upsert(new BucketSpec("api", 2, 20, 2, 2_000));
        AbstractBucketRuntime after = registry.get("api");

        assertTrue(result.applied());
        assertSame(before, after);
        assertEquals(2, after.spec().version());
        assertEquals(20, after.spec().capacity());
        assertSame(after, registry.getByHashUnique(after.bucketKey()));
    }

    @Test
    void upsertTypeChangeReplacesRuntimeImplementation() {
        BucketSpec initial = new BucketSpec("adaptive", 1, 10, 1, 1_000);
        BucketRegistry registry = new BucketRegistry(Map.of(initial.name(), initial));

        AbstractBucketRuntime before = registry.get("adaptive");
        BucketSpec replacement = new BucketSpec(
            "adaptive",
            2,
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

        BucketRegistry.UpsertResult result = registry.upsert(replacement);
        AbstractBucketRuntime after = registry.get("adaptive");

        assertTrue(result.applied());
        assertNotSame(before, after);
        assertInstanceOf(AdaptiveRateRuntime.class, after);
        assertEquals(BucketType.ADAPTIVE_RATE, after.spec().type());
    }

    @Test
    void staleVersionIsIgnored() {
        BucketSpec initial = new BucketSpec("stale", 3, 10, 1, 1_000);
        BucketRegistry registry = new BucketRegistry(Map.of(initial.name(), initial));

        BucketRegistry.UpsertResult result = registry.upsert(new BucketSpec("stale", 2, 99, 9, 9_000));

        assertFalse(result.applied());
        assertEquals(3, registry.get("stale").spec().version());
        assertEquals(10, registry.get("stale").spec().capacity());
    }
}
