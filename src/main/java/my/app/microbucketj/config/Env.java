package my.app.microbucketj.config;

public final class Env {

  private Env() {
  }

  public static String getEnv(String key, String def) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? def : v.trim();
  }

  public static int getEnvInteger(String key, int def) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? def : Integer.parseInt(v.trim());
  }

  public static long getEnvLong(String key, long def) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? def : Long.parseLong(v.trim());
  }

  public static boolean getEnvBoolean(String key, boolean def) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
  }
}
