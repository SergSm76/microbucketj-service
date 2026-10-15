package my.app.microbucketj.bucket;

import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.utils.Hashing;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;

public abstract class AbstractBucketRuntime {

  private static final VarHandle SPEC;

  static {
    try {
      SPEC = MethodHandles.lookup().findVarHandle(AbstractBucketRuntime.class, "spec", BucketSpec.class);
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final long bucketKey;
  protected volatile BucketSpec spec;

  protected AbstractBucketRuntime(BucketSpec spec) {
    this.spec = spec;
    byte[] nb = spec.name().getBytes(StandardCharsets.UTF_8);
    this.bucketKey = Hashing.fnv1a64(nb, 0, nb.length);
  }

  public final long bucketKey() {
    return bucketKey;
  }

  public final BucketSpec spec() {
    return spec;
  }

  protected final boolean compareAndSetSpec(BucketSpec cur, BucketSpec next) {
    return SPEC.compareAndSet(this, cur, next);
  }

  protected final BucketSpec getVolatileSpec() {
    return (BucketSpec) SPEC.getVolatile(this);
  }

  public abstract boolean updateSpec(BucketSpec newSpec);

  public abstract AcquireResult acquireAsPrimary(long permits);

  public AcquireResult acquire1AsPrimary() {
    return acquireAsPrimary(1L);
  }

  public abstract ReportResult reportAsPrimary(boolean success);

  public abstract void applyReplica(BucketState s);

  public abstract BucketState snapshot();

  public abstract long seq();

  public record AcquireResult(byte status, long remaining, long retryAfterMs, boolean mutated) {
  }

  public record ReportResult(byte status, boolean mutated) {
  }
}
