package my.app.microbucketj.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BucketSpecParser {

  private BucketSpecParser() {}

  public static Map<String, BucketSpec> parseBuckets(String raw) {
    if (raw == null || raw.isBlank()) return Map.of();

    Map<String, BucketSpec> out = new LinkedHashMap<>();

    for (String part : raw.split(";")) {
      String s = part.trim();
      if (s.isEmpty()) continue;

      int colon = s.indexOf(':');
      if (colon <= 0) throw new IllegalArgumentException("Bad bucket entry: " + s);

      String name = s.substring(0, colon).trim();
      String params = s.substring(colon + 1).trim();

      long ver = 1;
      BucketType type = BucketType.TOKEN;

      long cap = -1;
      long refill = -1;
      long period = -1;
      long stepPeriodMs = 0;
      long notUsedMs = 0;
      StringBuilder stepCapsRaw = null;
      boolean collectingStepCaps = false;

      long adaptiveWindowMs = 10_000L;
      int adaptiveMinRequests = 100;
      int adaptiveFailureRatePct = 50;
      int adaptiveMinFactorPct = 10;
      int adaptiveDecreaseStepPct = 50;
      int adaptiveIncreaseStepPct = 10;

      for (String kv : params.split(",")) {
        String t = kv.trim();
        if (t.isEmpty()) continue;

        int eq = t.indexOf('=');
        if (eq <= 0) {
          if (collectingStepCaps && stepCapsRaw != null) {
            stepCapsRaw.append(',').append(t);
            continue;
          }
          throw new IllegalArgumentException("Bad bucket param: " + t);
        }

        String k = t.substring(0, eq).trim();
        String v = t.substring(eq + 1).trim();

        if (isStepCapsKey(k)) {
          collectingStepCaps = true;
          if (stepCapsRaw == null) stepCapsRaw = new StringBuilder();
          if (stepCapsRaw.length() > 0) stepCapsRaw.append(',');
          stepCapsRaw.append(v);
          continue;
        }
        collectingStepCaps = false;

        switch (k) {
          case "ver", "version", "v" -> ver = Long.parseLong(v);
          case "type" -> type = BucketType.parse(v);

          case "cap" -> cap = Long.parseLong(v);
          case "refill" -> refill = Long.parseLong(v);
          case "periodMs" -> period = Long.parseLong(v);
          case "step-period", "stepPeriod", "stepPeriodMs" -> stepPeriodMs = Long.parseLong(v);
          case "not-used", "notUsed", "notUsedMs" -> notUsedMs = Long.parseLong(v);

          case "windowMs", "adaptiveWindowMs" -> adaptiveWindowMs = Long.parseLong(v);
          case "minRequests", "adaptiveMinRequests" -> adaptiveMinRequests = Integer.parseInt(v);
          case "failureRatePct", "adaptiveFailureRatePct" -> adaptiveFailureRatePct = Integer.parseInt(v);
          case "minFactorPct", "adaptiveMinFactorPct" -> adaptiveMinFactorPct = Integer.parseInt(v);
          case "decreaseStepPct", "adaptiveDecreaseStepPct", "degradeStepPct" -> adaptiveDecreaseStepPct = Integer.parseInt(v);
          case "increaseStepPct", "adaptiveIncreaseStepPct", "recoverStepPct" -> adaptiveIncreaseStepPct = Integer.parseInt(v);
          default -> throw new IllegalArgumentException("Unknown bucket param: " + k);
        }
      }

      long[] steps = null;
      if (stepCapsRaw != null && !stepCapsRaw.toString().isBlank()) {
        steps = parseStepCapacities(stepCapsRaw.toString());
      }
      out.put(name, new BucketSpec(name, ver, type,
          cap, refill, period, steps, stepPeriodMs, notUsedMs,
          type == BucketType.ADAPTIVE_RATE ? adaptiveWindowMs : 0L,
          type == BucketType.ADAPTIVE_RATE ? adaptiveMinRequests : 0,
          type == BucketType.ADAPTIVE_RATE ? adaptiveFailureRatePct : 0,
          type == BucketType.ADAPTIVE_RATE ? adaptiveMinFactorPct : 0,
          type == BucketType.ADAPTIVE_RATE ? adaptiveDecreaseStepPct : 0,
          type == BucketType.ADAPTIVE_RATE ? adaptiveIncreaseStepPct : 0));
    }

    return out;
  }

  private static boolean isStepCapsKey(String k) {
    return "step-capacities".equals(k) || "stepCapacities".equals(k) || "stepCaps".equals(k);
  }

  private static long[] parseStepCapacities(String raw) {
    String s = raw.trim();
    if (s.startsWith("[") && s.endsWith("]") && s.length() >= 2) s = s.substring(1, s.length() - 1).trim();
    if (s.isEmpty()) return new long[0];

    s = s.replace('|', ',').replace('/', ',').replace(':', ',');
    String[] parts = s.split(",");
    List<Long> vals = new ArrayList<>(parts.length);
    for (String p : parts) {
      String t = p.trim();
      if (!t.isEmpty()) vals.add(Long.parseLong(t));
    }
    long[] out = new long[vals.size()];
    for (int i = 0; i < vals.size(); i++) out[i] = vals.get(i);
    return out;
  }
}
