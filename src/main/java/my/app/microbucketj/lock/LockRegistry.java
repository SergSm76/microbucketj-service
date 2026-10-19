package my.app.microbucketj.lock;

import my.app.microbucketj.api.model.LockEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Реестр блокировок. Семантика — как у ShedLock:
 * acquire  = INSERT ... ON CONFLICT(name) DO UPDATE SET ... WHERE lock_until <= now
 * update   = продление (extend) только владельцем или по истечении
 * release  = DELETE WHERE name = ? AND locked_by = ?
 */
public final class LockRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(LockRegistry.class);

  /**
   * applied=true — операция применена локально (нужна репликация/ответ OK).
   */
  public record LockResult(boolean applied, LockEntry current) {
  }

  private final ConcurrentHashMap<String, LockEntry> locks = new ConcurrentHashMap<>();
  private final LongSupplier clock;

  public LockRegistry() {
    this(System::currentTimeMillis);
  }

  public LockRegistry(LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * Захват блокировки: успешен, если записи нет или она истекла.
   */
  public LockResult acquire(String name, long lockUntilMs, String lockedBy) {
    long now = clock.getAsLong();
    LockEntry candidate = new LockEntry(name, lockUntilMs, now, lockedBy);
    // TODO заменить бесконечный цикл
    for (; ; ) {
      LockEntry cur = locks.putIfAbsent(name, candidate);
      if (cur == null) {
        LOG.debug("Операция lock-acquire применена: lock={} by={} until={}", name, lockedBy, lockUntilMs);
        return new LockResult(true, candidate);
      }
      if (cur.isExpired(now)) {
        if (locks.replace(name, cur, candidate)) {
          LOG.debug("Операция lock-acquire перехватила истекшую блокировку: lock={} prevBy={} newBy={}",
              name, cur.lockedBy(), lockedBy);
          return new LockResult(true, candidate);
        }
        continue; // CAS-повтор
      }
      return new LockResult(false, cur); // блокировка держится другим владельцем
    }
  }

  /**
   * Продление: только текущий владелец (или если блокировка уже истекла).
   */
  public LockResult update(String name, long lockUntilMs, String lockedBy) {
    long now = clock.getAsLong();
    // TODO заменить бесконечный цикл
    for (; ; ) {
      LockEntry cur = locks.get(name);
      if (cur == null) return new LockResult(false, null);
      if (!cur.isOwnedBy(lockedBy) && !cur.isExpired(now)) return new LockResult(false, cur);
      LockEntry upd = new LockEntry(name, lockUntilMs, cur.lockedAtMs(), lockedBy);
      if (locks.replace(name, cur, upd)) return new LockResult(true, upd);
    }
  }

  /**
   * Освобождение: только владелец (или принудительно, если истекла).
   */
  public LockResult release(String name, String lockedBy) {
    long now = clock.getAsLong();
    // TODO заменить бесконечный цикл
    for (; ; ) {
      LockEntry cur = locks.get(name);
      if (cur == null) return new LockResult(false, null);
      if (!cur.isOwnedBy(lockedBy) && !cur.isExpired(now)) return new LockResult(false, cur);
      if (locks.remove(name, cur)) return new LockResult(true, cur);
    }
  }

  public LockEntry get(String name) {
    return locks.get(name);
  }

  /**
   * Административное удаление (аналог DELETE FROM shedlock WHERE name = ?).
   */
  public boolean delete(String name) {
    return locks.remove(name) != null;
  }

  /**
   * Применение реплики от другого узла кластера (без re-broadcast).
   */
  public void applyRemote(LockEntry entry) {
    for (; ; ) {
      LockEntry cur = locks.get(entry.name());
      if (cur == null) {
        if (locks.putIfAbsent(entry.name(), entry) == null) return;
        continue;
      }
      // last-writer-wins по lockUntilMs/lockedAtMs, как версионирование бакетов
      if (entry.lockedAtMs() >= cur.lockedAtMs() && locks.replace(entry.name(), cur, entry)) return;
      return;
    }
  }

  public void removeRemote(String name) {
    locks.remove(name);
  }

  /**
   * Периодическая очистка истёкших записей (аналог not-used-reset у бакетов).
   */
  public int cleanupExpired() {
    long now = clock.getAsLong();
    int removed = 0;
    for (var it = locks.entrySet().iterator(); it.hasNext(); ) {
      var e = it.next();
      if (e.getValue().isExpired(now)) {
        it.remove();
        removed++;
      }
    }
    if (removed > 0) LOG.info("Очистка истёкших блокировок: removed={}", removed);
    return removed;
  }

  public int size() {
    return locks.size();
  }
}