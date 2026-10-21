package my.app.microbucketj.lock;

import my.app.microbucketj.api.model.LockEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
   * Период ожидания перед удалением истёкшей блокировки.
   * Блокировка будет удалена только если её lockUntilMs истёк
   * более или равно CLEANUP_GRACE_PERIOD_MS назад относительно текущего времени.
   * Это оставляет "буфер" для возможного продления или диагностики.
   */
  private static final long CLEANUP_GRACE_PERIOD_MS = TimeUnit.HOURS.toMillis(1);

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
   * Использует атомарную операцию compute для исключения CAS-циклов.
   */
  public LockResult acquire(String name, long lockUntilMs, String lockedBy) {
    long now = clock.getAsLong();
    LockEntry candidate = new LockEntry(name, lockUntilMs, now, lockedBy);

    // compute гарантирует атомарность для конкретного ключа
    LockEntry result = locks.compute(name, (k, cur) -> {
      if (cur == null || cur.isExpired(now)) {
        return candidate; // успешный захват
      }
      return cur; // блокировка удерживается другим
    });

    boolean applied = (result == candidate);
    LockResult lockResult = new LockResult(applied, result);

    if (applied) {
      LOG.debug("Операция lock-acquire применена: lock={} by={} until={}", name, lockedBy, lockUntilMs);
    } else {
      LOG.debug("Блокировка {} уже держится: owner={}, until={}", name, result.lockedBy(), result.lockUntilMs());
    }

    return lockResult;
  }

  /**
   * Продление: только текущий владелец (или если блокировка уже истекла).
   * Использует атомарную операцию compute для исключения CAS-циклов.
   */
  public LockResult update(String name, long lockUntilMs, String lockedBy) {
    long now = clock.getAsLong();

    // Массив для передачи результата из лямбды (аналог holder)
    final LockEntry[] candidate = new LockEntry[1];

    LockEntry result = locks.compute(name, (k, cur) -> {
      if (cur == null) {
        return null; // не обновляем, если записи нет
      }
      // Отказ: не владелец и блокировка ещё действует
      if (!cur.isOwnedBy(lockedBy) && !cur.isExpired(now)) {
        return cur;
      }
      // Успешное обновление
      candidate[0] = new LockEntry(name, lockUntilMs, cur.lockedAtMs(), lockedBy);
      return candidate[0];
    });

    if (result == null) {
      LOG.debug("Попытка обновить несуществующую блокировку: {}", name);
      return new LockResult(false, null);
    }

    boolean applied = (result == candidate[0]);
    LockResult lockResult = new LockResult(applied, result);

    if (applied) {
      LOG.debug("Блокировка {} успешно обновлена до {}", name, lockUntilMs);
    } else {
      LOG.debug("Отказ в обновлении блокировки {}: владелец={}, запрошен={}",
          name, result.lockedBy(), lockedBy);
    }

    return lockResult;
  }

  /**
   * Освобождение: только владелец (или принудительно, если истекла).
   * Использует атомарную операцию compute для исключения CAS-циклов.
   */
  public LockResult release(String name, String lockedBy) {
    long now = clock.getAsLong();

    // Holder для передачи состояния из лямбды
    final ReleaseHolder holder = new ReleaseHolder();

    locks.compute(name, (k, cur) -> {
      if (cur == null) {
        holder.wasPresent = false;
        return null;
      }
      holder.wasPresent = true;
      // Отказ: не владелец и блокировка ещё действует
      if (!cur.isOwnedBy(lockedBy) && !cur.isExpired(now)) {
        holder.rejected = true;
        holder.entry = cur;
        return cur;
      }
      // Успешное освобождение
      holder.entry = cur;
      return null; // удаляем из map
    });

    if (!holder.wasPresent) {
      LOG.debug("Попытка освободить несуществующую блокировку: {}", name);
      return new LockResult(false, null);
    }

    if (holder.rejected) {
      LOG.debug("Отказ в освобождении блокировки {}: владелец={}, запрошен={}",
          name, holder.entry.lockedBy(), lockedBy);
      return new LockResult(false, holder.entry);
    }

    LOG.debug("Блокировка {} успешно освобождена владельцем {}", name, lockedBy);
    return new LockResult(true, holder.entry);
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
   * Использует атомарный compute для last-writer-wins стратегии.
   */
  public void applyRemote(LockEntry entry) {
    locks.compute(entry.name(), (k, cur) -> {
      if (cur == null) {
        return entry; // первая запись
      }
      // last-writer-wins по lockedAtMs, как версионирование бакетов
      if (entry.lockedAtMs() >= cur.lockedAtMs()) {
        return entry; // новая запись новее
      }
      return cur; // оставляем текущую
    });
  }

  public void removeRemote(String name) {
    locks.remove(name);
  }

  /**
   * Периодическая очистка истёкших записей.
   * Удаляет блокировки, у которых срок окончания действия (lock_until)
   * истёк более или равным час назад относительно текущего времени.
   * <p>
   * Условие удаления: lockUntilMs &lt;= (now - CLEANUP_GRACE_PERIOD_MS)
   */
  public int cleanupExpired() {
    LOG.debug("Запуск поиска истекших блокировок для очистки");
    long now = clock.getAsLong();
    long threshold = now - CLEANUP_GRACE_PERIOD_MS;
    int removed = 0;
    for (var it = locks.entrySet().iterator(); it.hasNext(); ) {
      var e = it.next();
      // удаляем блокировку только если её срок истёк >= CLEANUP_GRACE_PERIOD_MS назад
      if (e.getValue().lockUntilMs() <= threshold) {
        it.remove();
        removed++;
      }
    }
    if (removed > 0) {
      LOG.info("Очистка истёкших блокировок: removed={}, threshold={}", removed, threshold);
    }
    return removed;
  }

  public int size() {
    return locks.size();
  }

  /**
   * Возвращает снимок текущего состояния всех блокировок.
   */
  public List<LockEntry> snapshot() {
    return new ArrayList<>(locks.values());
  }

  /**
   * Вспомогательный класс для передачи состояния из лямбды compute().
   */
  private static class ReleaseHolder {
    LockEntry entry;
    boolean wasPresent = true;
    boolean rejected = false;
  }
}
