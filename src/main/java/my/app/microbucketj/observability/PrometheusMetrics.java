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
  private final IntSupplier lockCountSupplier;
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
  private final LongAdder apiLockAcquireHeldTotal = new LongAdder();
  private final LongAdder apiLockAcquireUnavailableTotal = new LongAdder();
  private final LongAdder apiLockAcquireBadRequestTotal = new LongAdder();
  private final LongAdder apiLockAcquireNotFoundTotal = new LongAdder();

  private final LongAdder apiLockReleaseTotal = new LongAdder();
  private final LongAdder apiLockReleaseOkTotal = new LongAdder();
  private final LongAdder apiLockReleaseHeldTotal = new LongAdder();
  private final LongAdder apiLockReleaseUnavailableTotal = new LongAdder();
  private final LongAdder apiLockReleaseBadRequestTotal = new LongAdder();
  private final LongAdder apiLockReleaseNotFoundTotal = new LongAdder();

  private final LongAdder apiLockUpdateTotal = new LongAdder();
  private final LongAdder apiLockUpdateOkTotal = new LongAdder();
  private final LongAdder apiLockUpdateHeldTotal = new LongAdder();
  private final LongAdder apiLockUpdateUnavailableTotal = new LongAdder();
  private final LongAdder apiLockUpdateBadRequestTotal = new LongAdder();
  private final LongAdder apiLockUpdateNotFoundTotal = new LongAdder();

  private final LongAdder apiLockDeleteTotal = new LongAdder();
  private final LongAdder apiLockDeleteOkTotal = new LongAdder();
  private final LongAdder apiLockDeleteUnavailableTotal = new LongAdder();
  private final LongAdder apiLockDeleteBadRequestTotal = new LongAdder();
  private final LongAdder apiLockDeleteNotFoundTotal = new LongAdder();

  private final LongAdder apiLockGetTotal = new LongAdder();
  private final LongAdder apiLockGetOkTotal = new LongAdder();
  private final LongAdder apiLockGetNotFoundTotal = new LongAdder();
  private final LongAdder apiLockGetBadRequestTotal = new LongAdder();


  public PrometheusMetrics(String nodeId,
                           IntSupplier bucketCountSupplier,
                           IntSupplier clusterNodeCountSupplier,
                           IntSupplier lockCountSupplier,
                           BooleanSupplier leaderSupplier) {
    this.nodeId = sanitizeLabel(Objects.requireNonNullElse(nodeId, "unknown"));
    this.bucketCountSupplier = bucketCountSupplier == null ? () -> 0 : bucketCountSupplier;
    this.clusterNodeCountSupplier = clusterNodeCountSupplier == null ? () -> 0 : clusterNodeCountSupplier;
    this.lockCountSupplier = lockCountSupplier == null ? () -> 0 : lockCountSupplier;
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

  // ИСПРАВЛЕННЫЕ МЕТОДЫ
  public void recordLockAcquire(byte status) {
    apiLockAcquireTotal.increment();
    switch (status) {
      case 5 -> apiLockAcquireOkTotal.increment();
      case 10 -> apiLockAcquireHeldTotal.increment(); // ST_LOCK_HELD
      case 3 -> apiLockAcquireUnavailableTotal.increment();
      case 6 -> apiLockAcquireBadRequestTotal.increment();
      case 4 -> apiLockAcquireNotFoundTotal.increment();
    }
  }

  public void recordLockRelease(byte status) {
    apiLockReleaseTotal.increment();
    switch (status) {
      case 5 -> apiLockReleaseOkTotal.increment();
      case 10 -> apiLockReleaseHeldTotal.increment();
      case 3 -> apiLockReleaseUnavailableTotal.increment();
      case 6 -> apiLockReleaseBadRequestTotal.increment();
      case 4 -> apiLockReleaseNotFoundTotal.increment();
    }
  }

  public void recordLockUpdate(byte status) {
    apiLockUpdateTotal.increment();
    switch (status) {
      case 5 -> apiLockUpdateOkTotal.increment();
      case 10 -> apiLockUpdateHeldTotal.increment();
      case 3 -> apiLockUpdateUnavailableTotal.increment();
      case 6 -> apiLockUpdateBadRequestTotal.increment();
      case 4 -> apiLockUpdateNotFoundTotal.increment();
    }
  }

  public void recordLockDelete(byte status) {
    apiLockDeleteTotal.increment();
    switch (status) {
      case 5 -> apiLockDeleteOkTotal.increment();
      case 3 -> apiLockDeleteUnavailableTotal.increment();
      case 6 -> apiLockDeleteBadRequestTotal.increment();
      case 4 -> apiLockDeleteNotFoundTotal.increment();
    }
  }

  public void recordLockGet(byte status) {
    apiLockGetTotal.increment();
    switch (status) {
      case 5 -> apiLockGetOkTotal.increment();
      case 4 -> apiLockGetNotFoundTotal.increment();
      case 6 -> apiLockGetBadRequestTotal.increment();
    }
  }

  public String render() {
    StringBuilder sb = new StringBuilder(4096);
    line(sb, "# HELP microbucketj_up Service health gauge");
    line(sb, "# TYPE microbucketj_up gauge");
    metric(sb, "microbucketj_up", 1);

    line(sb, "# HELP microbucketj_bucket_count Configured buckets");
    line(sb, "# TYPE microbucketj_bucket_count gauge");
    metric(sb, "microbucketj_bucket_count", bucketCountSupplier.getAsInt());

    line(sb, "# HELP microbucketj_lock_count Active locks in registry on this node");
    line(sb, "# TYPE microbucketj_lock_count gauge");
    metric(sb, "microbucketj_lock_count", lockCountSupplier.getAsInt());

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

    counter(sb, "microbucketj_api_lock_acquire_total", apiLockAcquireTotal.sum());
    counter(sb, "microbucketj_api_lock_acquire_ok_total", apiLockAcquireOkTotal.sum());
    counter(sb, "microbucketj_api_lock_acquire_held_total", apiLockAcquireHeldTotal.sum());
    counter(sb, "microbucketj_api_lock_acquire_unavailable_total", apiLockAcquireUnavailableTotal.sum());
    counter(sb, "microbucketj_api_lock_acquire_bad_request_total", apiLockAcquireBadRequestTotal.sum());
    counter(sb, "microbucketj_api_lock_acquire_not_found_total", apiLockAcquireNotFoundTotal.sum());

    counter(sb, "microbucketj_api_lock_release_total", apiLockReleaseTotal.sum());
    counter(sb, "microbucketj_api_lock_release_ok_total", apiLockReleaseOkTotal.sum());
    counter(sb, "microbucketj_api_lock_release_held_total", apiLockReleaseHeldTotal.sum());
    counter(sb, "microbucketj_api_lock_release_unavailable_total", apiLockReleaseUnavailableTotal.sum());
    counter(sb, "microbucketj_api_lock_release_bad_request_total", apiLockReleaseBadRequestTotal.sum());
    counter(sb, "microbucketj_api_lock_release_not_found_total", apiLockReleaseNotFoundTotal.sum());

    counter(sb, "microbucketj_api_lock_update_total", apiLockUpdateTotal.sum());
    counter(sb, "microbucketj_api_lock_update_ok_total", apiLockUpdateOkTotal.sum());
    counter(sb, "microbucketj_api_lock_update_held_total", apiLockUpdateHeldTotal.sum());
    counter(sb, "microbucketj_api_lock_update_unavailable_total", apiLockUpdateUnavailableTotal.sum());
    counter(sb, "microbucketj_api_lock_update_bad_request_total", apiLockUpdateBadRequestTotal.sum());
    counter(sb, "microbucketj_api_lock_update_not_found_total", apiLockUpdateNotFoundTotal.sum());

    counter(sb, "microbucketj_api_lock_delete_total", apiLockDeleteTotal.sum());
    counter(sb, "microbucketj_api_lock_delete_ok_total", apiLockDeleteOkTotal.sum());
    counter(sb, "microbucketj_api_lock_delete_unavailable_total", apiLockDeleteUnavailableTotal.sum());
    counter(sb, "microbucketj_api_lock_delete_bad_request_total", apiLockDeleteBadRequestTotal.sum());
    counter(sb, "microbucketj_api_lock_delete_not_found_total", apiLockDeleteNotFoundTotal.sum());

    counter(sb, "microbucketj_api_lock_get_total", apiLockGetTotal.sum());
    counter(sb, "microbucketj_api_lock_get_ok_total", apiLockGetOkTotal.sum());
    counter(sb, "microbucketj_api_lock_get_not_found_total", apiLockGetNotFoundTotal.sum());
    counter(sb, "microbucketj_api_lock_get_bad_request_total", apiLockGetBadRequestTotal.sum());

    line(sb, "# HELP microbucketj_node_info Static node info");
    line(sb, "# TYPE microbucketj_node_info gauge");
    line(sb, "microbucketj_node_info{nodeId=\"" + nodeId + "\"} 1");

    return sb.toString();
  }
}