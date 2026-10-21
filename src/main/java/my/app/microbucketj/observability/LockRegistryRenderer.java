package my.app.microbucketj.observability;

import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.lock.LockRegistry;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Рендерер для вывода состояния LockRegistry.
 * Поддерживает два формата: text (табличный, для человека) и json.
 */
public final class LockRegistryRenderer {

  private final LockRegistry lockRegistry;
  private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

  public LockRegistryRenderer(LockRegistry lockRegistry) {
    this.lockRegistry = lockRegistry;
  }

  /**
   * Возвращает отфильтрованный список блокировок.
   *
   * @param nameFilter строка для поиска LIKE '%value%', null для получения всех
   */
  public List<LockEntry> getFilteredSnapshot(String nameFilter) {
    List<LockEntry> snapshot = lockRegistry.snapshot();

    if (nameFilter == null || nameFilter.isBlank()) {
      return snapshot;
    }

    String lowerFilter = nameFilter.toLowerCase();
    List<LockEntry> filtered = new ArrayList<>();
    for (LockEntry e : snapshot) {
      if (e.name().toLowerCase().contains(lowerFilter)) {
        filtered.add(e);
      }
    }
    return filtered;
  }

  /**
   * Рендерит список блокировок в JSON формат.
   */
  public String renderJson(List<LockEntry> snapshot) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (LockEntry e : snapshot) {
      if (!first) sb.append(",");
      sb.append("{");
      sb.append("\"name\":\"").append(escapeJson(e.name())).append("\",");
      sb.append("\"lockedBy\":\"").append(escapeJson(e.lockedBy())).append("\",");
      sb.append("\"lockedAtMs\":").append(e.lockedAtMs()).append(",");
      sb.append("\"lockUntilMs\":").append(e.lockUntilMs()).append(",");
      sb.append("\"expired\":").append(e.isExpired(System.currentTimeMillis()));
      sb.append("}");
      first = false;
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Рендерит список блокировок в табличный text формат (для чтения глазами в терминале).
   */
  public String renderText(List<LockEntry> snapshot) {
    StringBuilder sb = new StringBuilder();
    sb.append("LockRegistry snapshot (").append(snapshot.size()).append(" locks):\n");

    String header = String.format("%-32s | %-20s | %-19s | %-19s | %s%n",
        "NAME", "LOCKED_BY", "LOCKED_AT", "LOCK_UNTIL", "EXPIRED");
    sb.append(header);

    String separator = String.format("%-32s-+-%-20s-+-%-19s-+-%-19s-+-%s%n",
        "--------------------------------", "--------------------", "-------------------", "-------------------", "-------");
    sb.append(separator);

    long now = System.currentTimeMillis();
    for (LockEntry e : snapshot) {
      String lockedAtStr = fmt.format(Instant.ofEpochMilli(e.lockedAtMs()));
      String lockUntilStr = fmt.format(Instant.ofEpochMilli(e.lockUntilMs()));
      String expiredStr = e.isExpired(now) ? "YES" : "NO ";
      sb.append(String.format("%-32s | %-20s | %-19s | %-19s | %s%n",
          e.name(), e.lockedBy(), lockedAtStr, lockUntilStr, expiredStr));
    }
    return sb.toString();
  }

  private String escapeJson(String s) {
    if (s == null) return "null";
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
