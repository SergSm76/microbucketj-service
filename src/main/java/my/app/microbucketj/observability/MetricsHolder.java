package my.app.microbucketj.observability;

public final class MetricsHolder {

  private static volatile PrometheusMetrics metrics;

  private MetricsHolder() {
  }

  public static void set(PrometheusMetrics value) {
    metrics = value;
  }

  public static PrometheusMetrics get() {
    return metrics;
  }
}
