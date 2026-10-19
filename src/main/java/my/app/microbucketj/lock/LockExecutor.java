package my.app.microbucketj.lock;

import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.replication.ReplicationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static my.app.microbucketj.utils.Hashing.fnv1a64;

/**
 * Общая логика выполнения lock-мутации на primary-узле с последующей репликацией.
 * Вызывается и API-операми, и forward-операми (token-канал).
 * <p>
 * Сценарий использования:
 * 1. API-оп: роутит запрос (forward / локально), если локально — вызывает {@link #executeOnPrimary}.
 * 2. Forward-оп: всегда на primary, сразу вызывает {@link #executeOnPrimary}.
 */
public final class LockExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(LockExecutor.class);

  private final BooleanSupplier isLeader;
  private final MembershipRuntime membership;
  private final LockRegistry locks;
  private final ReplicationService replication;

  public LockExecutor(BooleanSupplier isLeader,
                      MembershipRuntime membership,
                      LockRegistry locks,
                      ReplicationService replication) {
    this.isLeader = (isLeader == null) ? (() -> false) : isLeader;
    this.membership = Objects.requireNonNull(membership, "membership");
    this.locks = Objects.requireNonNull(locks, "locks");
    this.replication = Objects.requireNonNull(replication, "replication");
  }

  /**
   * Результат выполнения операции на primary.
   */
  public record Result(boolean applied, byte status, LockEntry current, boolean deleted) {
  }

  /**
   * Функциональный интерфейс: операция над LockRegistry, возвращающая LockResult.
   */
  @FunctionalInterface
  public interface Operation {
    LockRegistry.LockResult execute(LockRegistry locks);
  }

  /**
   * Выполняет операцию локально (этот узел должен быть primary) и реплицирует.
   *
   * @param lockName      имя блокировки
   * @param op            локальная операция над LockRegistry
   * @param successStatus статус при успешном применении (обычно ST_OK)
   * @param deleted       true для операций, удаляющих запись (release, delete)
   */
  public CompletableFuture<Result> executeOnPrimary(String lockName,
                                                    Operation op,
                                                    byte successStatus,
                                                    boolean deleted) {
    long lockHash = fnv1a64(lockName);
    LOG.debug("Запуск операции на primary: lock={} hash={} deleted={}", lockName, lockHash, deleted);

    var snap = membership.snapshot();
    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (!primary.id().equals(view.self().id())) {
      LOG.debug("Узел не является primary для лока {}: primary={}", lockName, primary.id());
      return CompletableFuture.completedFuture(
          new Result(false, ApiProtocol.ST_UNAVAILABLE, null, deleted));
    }

    LockRegistry.LockResult res;
    try {
      res = op.execute(locks);
    } catch (IllegalArgumentException e) {
      LOG.debug("Ошибка валидации при выполнении операции для лока {}: {}", lockName, e.getMessage());
      return CompletableFuture.completedFuture(
          new Result(false, ApiProtocol.ST_BAD_REQUEST, null, deleted));
    } catch (Exception e) {
      LOG.error("Непредвиденная ошибка при выполнении операции для лока {}", lockName, e);
      return CompletableFuture.completedFuture(
          new Result(false, ApiProtocol.ST_UNAVAILABLE, null, deleted));
    }

    if (!res.applied()) {
      byte status = (res.current() == null) ? ApiProtocol.ST_NOT_FOUND : ApiProtocol.ST_LOCK_HELD;
      LOG.debug("Операция для лока {} отклонена локально: status={}", lockName, status);
      return CompletableFuture.completedFuture(new Result(false, status, res.current(), deleted));
    }

    LOG.debug("Операция для лока {} применена локально, запускаем репликацию", lockName);
    CompletableFuture<Boolean> replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, lockName,
        res.current(), deleted);

    final LockEntry current = res.current();
    return replF.handle((ok, err) -> {
      if (err != null || !Boolean.TRUE.equals(ok)) {
        LOG.warn("Ошибка репликации для лока {}: {}", lockName, err != null ? err.getMessage() : "ok=false");
        return new Result(false, ApiProtocol.ST_UNAVAILABLE, current, deleted);
      }
      LOG.debug("Репликация для лока {} завершена успешно", lockName);
      return new Result(true, successStatus, current, deleted);
    });
  }

  /**
   * Проверка: единственный узел в кластере, но не лидер → операция невозможна.
   */
  public boolean isSingleNodeNonLeader() {
    var snap = membership.snapshot();
    return !isLeader.getAsBoolean() && snap.nodes().size() <= 1;
  }

  /**
   * Возвращает primary-узел для данной блокировки (для forward-решений).
   */
  public Node primaryFor(String lockName) {
    return membership.snapshot().view().pickPrimary(fnv1a64(lockName));
  }

  /**
   * Является ли self primary для данной блокировки?
   */
  public boolean isSelfPrimary(String lockName) {
    var snap = membership.snapshot();
    Node primary = snap.view().pickPrimary(fnv1a64(lockName));
    return primary.id().equals(snap.view().self().id());
  }

  public LockRegistry locks() {
    return locks;
  }
}
