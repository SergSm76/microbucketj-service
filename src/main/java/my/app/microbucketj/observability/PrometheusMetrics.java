package my.app.microbucketj.observability;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

public final class PrometheusMetrics {

  private final String nodeId;
  private final IntSupplier bucketCountSupplier;
  private final IntSupplier clusterNodeCountSupplier;
  private final BooleanSupplier leaderSupplier;

  private final LongAdder apiAcquireTotal = new LongAdder();
  private final LongAdder apiAcquireAllowedTotal = new LongAdder();
  private final LongAdder apiAcquireDeniedTotal = new LongAdder();
  private final LongAdder apiAcquireUnavailableTotal = new LongAdder();
  private final LongAdder apiAcquireNotFoundTotal = new LongAdder();
  private final LongAdder apiAcquireCollisionTotal = new LongAdder();

  private final LongAdder apiReportTotal = new LongAdder();
  private final LongAdder apiReportOkTotal = new LongAdder();
  private final LongAdder apiReportUnavailableTotal = new LongAdder();
  private final LongAdder apiReportNotFoundTotal = new LongAdder();
  private final LongAdder apiReportCollisionTotal = new LongAdder();

  private final LongAdder apiBucketGetTotal = new LongAdder();
  private final LongAdder apiBucketGetOkTotal = new LongAdder();
  private final LongAdder apiBucketGetNotFoundTotal = new LongAdder();
  private final LongAdder apiBucketGetBadRequestTotal = new LongAdder();

  private final LongAdder apiBucketUpsertTotal = new LongAdder();
  private final LongAdder apiBucketUpsertOkTotal = new LongAdder();
  private final LongAdder apiBucketUpsertRedirectTotal = new LongAdder();
  private final LongAdder apiBucketUpsertUnavailableTotal = new LongAdder();
  private final LongAdder apiBucketUpsertBadRequestTotal = new LongAdder();

  private final LongAdder apiLockAcquireTotal = new LongAdder();
  private final LongAdder apiLockAcquireOkTotal = new LongAdder();
  private final LongAdder apiLockAcquireRedirectTotal = new LongAdder();
  private final LongAdder apiLockAcquireUnavailableTotal = new LongAdder();
  private final LongAdder apiLockAcquireBadRequestTotal = new LongAdder();

  public PrometheusMetrics(String nodeId,
                           IntSupplier bucketCountSupplier,
                           IntSupplier clusterNodeCountSupplier,
                           BooleanSupplier leaderSupplier) {
    this.nodeId = sanitizeLabel(Objects.requireNonNullElse(nodeId, "unknown"));
    this.bucketCountSupplier = bucketCountSupplier == null ? () -> 0 : bucketCountSupplier;
    this.clusterNodeCountSupplier = clusterNodeCountSupplier == null ? () -> 0 : clusterNodeCountSupplier;
    this.leaderSupplier = leaderSupplier == null ? () -> false : leaderSupplier;
  }

  private static void counter(StringBuilder sb, String name, long value) {
    line(sb, "# HELP " + name + ' ' + name.replace('_', ' '));
    line(sb, "# TYPE " + name + " counter");
    metric(sb, name, value);
  }

  private static void metric(StringBuilder sb, String name, long value) {
    line(sb, name + ' ' + value);
  }

  private static void line(StringBuilder sb, String value) {
    sb.append(value).append('\n');
  }

  private static String sanitizeLabel(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .toLowerCase(Locale.ROOT);
  }

  public void recordAcquire(byte status) {
    apiAcquireTotal.increment();
    switch (status) {
      case 1 -> apiAcquireAllowedTotal.increment();
      case 0 -> apiAcquireDeniedTotal.increment();
      case 3 -> apiAcquireUnavailableTotal.increment();
      case 4 -> apiAcquireNotFoundTotal.increment();
      case 7 -> apiAcquireCollisionTotal.increment();
      default -> throw new IllegalArgumentException("Unknown Acquire status: " + status);
    }
  }

  public void recordReport(byte status) {
    apiReportTotal.increment();
    switch (status) {
      case 5 -> apiReportOkTotal.increment();
      case 3 -> apiReportUnavailableTotal.increment();
      case 4 -> apiReportNotFoundTotal.increment();
      case 7 -> apiReportCollisionTotal.increment();
      default -> throw new IllegalArgumentException("Unknown Report status: " + status);
    }
  }

  public void recordBucketGet(byte status) {
    apiBucketGetTotal.increment();
    switch (status) {
      case 5 -> apiBucketGetOkTotal.increment();
      case 4 -> apiBucketGetNotFoundTotal.increment();
      case 6 -> apiBucketGetBadRequestTotal.increment();
      default -> throw new IllegalArgumentException("Unknown BucketGet status: " + status);
    }
  }

  public void recordBucketUpsert(byte status) {
    apiBucketUpsertTotal.increment();
    switch (status) {
      case 5 -> apiBucketUpsertOkTotal.increment();
      case 10 -> apiBucketUpsertRedirectTotal.increment();
      case 3 -> apiBucketUpsertUnavailableTotal.increment();
      case 6 -> apiBucketUpsertBadRequestTotal.increment();
      default -> throw new IllegalArgumentException("Unknown BucketUpsert status: " + status);
    }
  }

  // locks_acquire_total{status}, locks_release_total{status}, locks_update_total{status},
  // locks_get_total{status}, locks_delete_total{status}, locks_active (gauge = registry.size())
  public void recordLockAcquire(byte status) {
    apiLockAcquireTotal.increment();
    switch (status) {
      case 5 -> apiLockAcquireOkTotal.increment();
      case 10 -> apiBucketUpsertRedirectTotal.increment();
      case 3 -> apiBucketUpsertUnavailableTotal.increment();
      case 6 -> apiBucketUpsertBadRequestTotal.increment();
      default -> throw new IllegalArgumentException("Unknown BucketUpsert status: " + status);
    }
  }
  public void recordLockRelease(byte status) { ... }
  public void recordLockUpdate(byte status)  { ... }
  public void recordLockGet(byte status)     { ... }
  public void recordLockDelete(byte status)  { ... }

  public String render() {
    StringBuilder sb = new StringBuilder(4096);
    line(sb, "# HELP microbucketj_up Service health gauge");
    line(sb, "# TYPE microbucketj_up gauge");
    metric(sb, "microbucketj_up", 1);

    line(sb, "# HELP microbucketj_bucket_count Configured buckets");
    line(sb, "# TYPE microbucketj_bucket_count gauge");
    metric(sb, "microbucketj_bucket_count", bucketCountSupplier.getAsInt());

    line(sb, "# HELP microbucketj_cluster_nodes Current cluster nodes in membership snapshot");
    line(sb, "# TYPE microbucketj_cluster_nodes gauge");
    metric(sb, "microbucketj_cluster_nodes", clusterNodeCountSupplier.getAsInt());

    line(sb, "# HELP microbucketj_is_leader Whether current node is leader (1/0)");
    line(sb, "# TYPE microbucketj_is_leader gauge");
    metric(sb, "microbucketj_is_leader", leaderSupplier.getAsBoolean() ? 1 : 0);

    counter(sb, "microbucketj_api_acquire_total", apiAcquireTotal.sum());
    counter(sb, "microbucketj_api_acquire_allowed_total", apiAcquireAllowedTotal.sum());
    counter(sb, "microbucketj_api_acquire_denied_total", apiAcquireDeniedTotal.sum());
    counter(sb, "microbucketj_api_acquire_unavailable_total", apiAcquireUnavailableTotal.sum());
    counter(sb, "microbucketj_api_acquire_not_found_total", apiAcquireNotFoundTotal.sum());
    counter(sb, "microbucketj_api_acquire_collision_total", apiAcquireCollisionTotal.sum());

    counter(sb, "microbucketj_api_report_total", apiReportTotal.sum());
    counter(sb, "microbucketj_api_report_ok_total", apiReportOkTotal.sum());
    counter(sb, "microbucketj_api_report_unavailable_total", apiReportUnavailableTotal.sum());
    counter(sb, "microbucketj_api_report_not_found_total", apiReportNotFoundTotal.sum());
    counter(sb, "microbucketj_api_report_collision_total", apiReportCollisionTotal.sum());

    counter(sb, "microbucketj_api_bucket_get_total", apiBucketGetTotal.sum());
    counter(sb, "microbucketj_api_bucket_get_ok_total", apiBucketGetOkTotal.sum());
    counter(sb, "microbucketj_api_bucket_get_not_found_total", apiBucketGetNotFoundTotal.sum());
    counter(sb, "microbucketj_api_bucket_get_bad_request_total", apiBucketGetBadRequestTotal.sum());

    counter(sb, "microbucketj_api_bucket_upsert_total", apiBucketUpsertTotal.sum());
    counter(sb, "microbucketj_api_bucket_upsert_ok_total", apiBucketUpsertOkTotal.sum());
    counter(sb, "microbucketj_api_bucket_upsert_redirect_total", apiBucketUpsertRedirectTotal.sum());
    counter(sb, "microbucketj_api_bucket_upsert_unavailable_total", apiBucketUpsertUnavailableTotal.sum());
    counter(sb, "microbucketj_api_bucket_upsert_bad_request_total", apiBucketUpsertBadRequestTotal.sum());

    line(sb, "# HELP microbucketj_node_info Static node info");
    line(sb, "# TYPE microbucketj_node_info gauge");
    line(sb, "microbucketj_node_info{nodeId=\"" + nodeId + "\"} 1");
    return sb.toString();
  }
}