package my.app.microbucketj.bucket;

import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.config.BucketType;
import my.app.microbucketj.utils.Hashing;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class BucketRegistry {

  private static final Object COLLISION = new Object();
  private final ConcurrentHashMap<String, AbstractBucketRuntime> runtimes = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, Object> hashIndex = new ConcurrentHashMap<>();
  public BucketRegistry(Map<String, BucketSpec> initial) {
    if (initial != null) {
      for (BucketSpec spec : initial.values()) {
        AbstractBucketRuntime rt = createRuntime(spec);
        runtimes.put(spec.name(), rt);
        indexHash(spec.name(), rt);
      }
    }
  }

  private static AbstractBucketRuntime createRuntime(BucketSpec spec) {
    return spec.type() == BucketType.ADAPTIVE_RATE
        ? new AdaptiveRateRuntime(spec)
        : new RateLimiterRuntime(spec);
  }

  private static long bucketHash(String bucketName) {
    if (bucketName == null) return 0L;
    byte[] b = bucketName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return Hashing.fnv1a64(b, 0, b.length);
  }

  public AbstractBucketRuntime get(String bucket) {
    if (bucket == null || bucket.isBlank()) return null;
    return runtimes.get(bucket);
  }

  public AbstractBucketRuntime getByHashUnique(long bucketHash) {
    Object v = hashIndex.get(bucketHash);
    return v instanceof AbstractBucketRuntime abr ? abr : null;
  }

  public boolean hasHash(long bucketHash) {
    return hashIndex.containsKey(bucketHash);
  }

  public boolean isCollisionHash(long bucketHash) {
    return hashIndex.get(bucketHash) == COLLISION;
  }

  public BucketSpec getSpec(String bucket) {
    AbstractBucketRuntime rt = get(bucket);
    return rt == null ? null : rt.spec();
  }

  public UpsertResult upsert(BucketSpec spec) {
    if (spec == null || spec.name() == null || spec.name().isBlank()) {
      return new UpsertResult(false, null);
    }

    final boolean[] applied = {false};
    final BucketSpec[] cur = {null};

    runtimes.compute(spec.name(), (name, existing) -> {
      if (existing == null) {
        applied[0] = true;
        AbstractBucketRuntime rt = createRuntime(spec);
        cur[0] = rt.spec();
        indexHash(name, rt);
        return rt;
      }

      BucketSpec existingSpec = existing.spec();
      if (spec.version() <= existingSpec.version()) {
        applied[0] = false;
        cur[0] = existingSpec;
        return existing;
      }

      if (existingSpec.type() == spec.type()) {
        applied[0] = existing.updateSpec(spec);
        cur[0] = existing.spec();
        indexHash(name, existing);
        return existing;
      }

      AbstractBucketRuntime replacement = createRuntime(spec);
      applied[0] = true;
      cur[0] = replacement.spec();
      indexHash(name, replacement);
      return replacement;
    });

    return new UpsertResult(applied[0], cur[0]);
  }

  private void indexHash(String bucketName, AbstractBucketRuntime rt) {
    long h = bucketHash(bucketName);
    hashIndex.compute(h, (k, prev) -> {
      if (prev == null) return rt;
      if (prev == COLLISION) return COLLISION;
      AbstractBucketRuntime other = (AbstractBucketRuntime) prev;
      String otherName = other.spec().name();
      if (otherName.equals(bucketName)) return rt;
      return COLLISION;
    });
  }

  public Set<String> bucketNames() {
    return runtimes.keySet();
  }

  public Collection<AbstractBucketRuntime> all() {
    return runtimes.values();
  }

  public record UpsertResult(boolean applied, BucketSpec current) {
  }
}
