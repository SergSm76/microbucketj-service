package my.app.microbucketj.api.model;

import java.nio.charset.StandardCharsets;

/**
 * Аналог строки таблицы shedlock:
 * (name VARCHAR(64) PK, lock_until TIMESTAMP, locked_at TIMESTAMP, locked_by VARCHAR(255))
 */
public record LockEntry(
    String name,        // VARCHAR(64), PRIMARY KEY
    long lockUntilMs,   // TIMESTAMP lock_until (epoch millis)
    long lockedAtMs,    // TIMESTAMP locked_at (epoch millis)
    String lockedBy     // VARCHAR(255) locked_by
) {
  public static final int MAX_NAME_BYTES = 64;
  public static final int MAX_OWNER_BYTES = 255;

  public LockEntry {
    if (name == null || name.isBlank()) throw new IllegalArgumentException("Lock name is blank");
    if (name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES)
      throw new IllegalArgumentException("Lock name exceeds " + MAX_NAME_BYTES + " bytes");
    if (lockedBy == null || lockedBy.isBlank()) throw new IllegalArgumentException("lockedBy is blank");
    if (lockedBy.getBytes(StandardCharsets.UTF_8).length > MAX_OWNER_BYTES)
      throw new IllegalArgumentException("lockedBy exceeds " + MAX_OWNER_BYTES + " bytes");
    if (lockUntilMs <= 0) throw new IllegalArgumentException("lockUntilMs must be > 0");
    if (lockedAtMs <= 0) lockedAtMs = System.currentTimeMillis();
  }

  public boolean isExpired(long nowMs) {
    return lockUntilMs <= nowMs;
  }

  public boolean isOwnedBy(String owner) {
    return lockedBy.equals(owner);
  }
}
