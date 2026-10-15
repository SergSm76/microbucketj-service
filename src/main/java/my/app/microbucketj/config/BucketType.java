package my.app.microbucketj.config;

public enum BucketType {

  TOKEN,
  ADAPTIVE_RATE;

  public static BucketType parse(String raw) {
    if (raw == null || raw.isBlank()) return TOKEN;
    String v = raw.trim().toLowerCase();
    return switch (v) {
      case "token", "rate", "rate_limiter", "ratelimiter" -> TOKEN;
      case "adaptive", "adaptive_rate", "adaptive-rate", "hybrid", "hybrid_rate", "hybrid-rate" -> ADAPTIVE_RATE;
      default -> throw new IllegalArgumentException("Unknown bucket type: " + raw);
    };
  }
}
