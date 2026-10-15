package my.app.microbucketj.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.protocol.ApiProtocol;

class SynchronizedBucketRuntimeTest {

    @Test
    void rateLimiterSerializesConcurrentAcquireOnSingleBucket() throws Exception {
        RateLimiterRuntime runtime = new RateLimiterRuntime(new BucketSpec("single-bucket", 1, 128, 1, 3_600_000));
        int threads = 8;
        int attemptsPerThread = 32;
        AtomicInteger allowed = new AtomicInteger();

        runConcurrently(threads, () -> {
            for (int i = 0; i < attemptsPerThread; i++) {
                if (runtime.acquire1AsPrimary().status() == ApiProtocol.ST_ALLOWED) {
                    allowed.incrementAndGet();
                }
            }
        });

        assertEquals(128, allowed.get());
        assertEquals((long) threads * attemptsPerThread, runtime.seq());
        assertEquals(0, runtime.snapshot().tokens());
    }

    @Test
    void adaptiveRuntimePreservesConcurrentReportsAndSnapshots() throws Exception {
        AdaptiveRateRuntime runtime = new AdaptiveRateRuntime(adaptiveSpec());
        int reporters = 6;
        int snapshotReaders = 2;
        int reportsPerThread = 200;
        int snapshotsPerThread = 200;
        List<ThrowingRunnable> tasks = new ArrayList<>();

        for (int t = 0; t < reporters; t++) {
            tasks.add(() -> {
                for (int i = 0; i < reportsPerThread; i++) {
                    runtime.reportAsPrimary((i & 1) == 0);
                }
            });
        }
        for (int t = 0; t < snapshotReaders; t++) {
            tasks.add(() -> {
                long prevSeq = -1;
                for (int i = 0; i < snapshotsPerThread; i++) {
                    BucketState snap = runtime.snapshot();
                    assertTrue(snap.seq() >= prevSeq);
                    assertTrue(snap.adaptiveRatePct() > 0 && snap.adaptiveRatePct() <= 100);
                    prevSeq = snap.seq();
                }
            });
        }

        runConcurrently(tasks);

        long expectedSeq = (long) reporters * reportsPerThread;
        BucketState snap = runtime.snapshot();
        assertEquals(expectedSeq, runtime.seq());
        assertEquals(expectedSeq, snap.seq());
        assertTrue(snap.adaptiveRatePct() > 0 && snap.adaptiveRatePct() <= 100);
    }

    @Test
    void rateLimiterIgnoresStaleReplicaState() {
        RateLimiterRuntime runtime = new RateLimiterRuntime(new BucketSpec("replica-test", 1, 10, 1, 3_600_000));

        runtime.acquireAsPrimary(3);
        BucketState current = runtime.snapshot();
        BucketState newer = new BucketState(
            current.bucket(),
            current.specVersion(),
            current.seq() + 5,
            current.type(),
            1,
            current.lastRefillNanos(),
            current.lastAccessNanos(),
            current.stepIndex(),
            current.stepStartNanos(),
            0,
            0L,
            0,
            0,
            0
        );

        runtime.applyReplica(newer);
        runtime.applyReplica(new BucketState(
            newer.bucket(),
            newer.specVersion(),
            newer.seq() - 1,
            newer.type(),
            9,
            newer.lastRefillNanos(),
            newer.lastAccessNanos(),
            newer.stepIndex(),
            newer.stepStartNanos(),
            0,
            0L,
            0,
            0,
            0
        ));

        BucketState snap = runtime.snapshot();
        assertEquals(newer.seq(), snap.seq());
        assertEquals(newer.tokens(), snap.tokens());
    }

    private static BucketSpec adaptiveSpec() {
        return new BucketSpec(
            "adaptive-single-bucket",
            1,
            BucketType.ADAPTIVE_RATE,
            100,
            100,
            3_600_000,
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

    private static void runConcurrently(int threads, ThrowingRunnable action) throws Exception {
        List<ThrowingRunnable> tasks = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            tasks.add(action);
        }
        runConcurrently(tasks);
    }

    private static void runConcurrently(List<ThrowingRunnable> tasks) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(tasks.size());
        CountDownLatch ready = new CountDownLatch(tasks.size());
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(tasks.size());
        try {
            for (ThrowingRunnable task : tasks) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    task.run();
                    return null;
                }));
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
