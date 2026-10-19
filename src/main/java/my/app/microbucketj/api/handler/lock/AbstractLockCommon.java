package my.app.microbucketj.api.handler.lock;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Базовый класс для lock-операций с маршрутизацией на primary-узел.
 * Паттерн аналогичен AbstractAcquireCommon:
 * - Если текущий узел — primary для данного lock name → выполнить локально + реплицировать
 * - Иначе → forward на primary через TokenPeerClient
 */
public abstract class AbstractLockCommon implements OpcodeHandler {

  protected final BooleanSupplier isLeader;
  protected final MembershipRuntime membership;
  protected final LockRegistry locks;
  protected final TokenPeerClient tokenPeers;
  protected final ReplicationService replication;

  protected AbstractLockCommon(BooleanSupplier isLeader,
                               MembershipRuntime membership,
                               LockRegistry locks,
                               TokenPeerClient tokenPeers,
                               ReplicationService replication) {
    this.isLeader = (isLeader == null) ? (() -> false) : isLeader;
    this.membership = Objects.requireNonNull(membership, "membership");
    this.locks = Objects.requireNonNull(locks, "locks");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
    this.replication = Objects.requireNonNull(replication, "replication");
  }

  /**
   * Основная логика маршрутизации write-операции.
   *
   * @param operation      функция, выполняющая операцию на primary (acquire/release/update/delete)
   * @param responseWriter функция, формирующая ответ клиенту
   */
  protected void runLockOperation(ChannelHandlerContext ctx,
                                  int clientRequestId,
                                  String lockName,
                                  LockOperation operation,
                                  ResponseWriter responseWriter) {
    if (lockName == null || lockName.isBlank()) {
      writeBadRequest(ctx, clientRequestId);
      return;
    }

    long lockHash = fnv1a64(lockName);

    // Проверка: если узел единственный в кластере, но не лидер — unavailable
    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      writeUnavailable(ctx, clientRequestId);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (primary.id().equals(view.self().id())) {
      // Текущий узел — primary, выполнить локально
      LockOperationResult result;
      try {
        result = operation.execute();
      } catch (Exception e) {
        writeUnavailable(ctx, clientRequestId);
        return;
      }

      if (!result.applied()) {
        writeResponse(ctx, result.status(), clientRequestId, result.entry());
        return;
      }

      // Репликация с ожиданием MIN_ACKS
      var replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, result.entry(), result.deleted());
      if (replF.isDone()) {
        boolean ok = replF.getNow(false);
        if (!ok) {
          writeUnavailable(ctx, clientRequestId);
        } else {
          writeResponse(ctx, result.status(), clientRequestId, result.entry());
        }
        return;
      }

      replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
        if (err != null || !ok) {
          writeUnavailable(ctx, clientRequestId);
        } else {
          writeResponse(ctx, result.status(), clientRequestId, result.entry());
        }
      }));
      return;
    }

    // Forward на primary
    forwardToPrimary(ctx, clientRequestId, primary, lockName, operation, responseWriter);
  }

  /**
   * Пересылка операции на primary-узел.
   * Переопределяется в каждом конкретном обработчике.
   */
  protected abstract void forwardToPrimary(ChannelHandlerContext ctx,
                                           int clientRequestId,
                                           Node primary,
                                           String lockName,
                                           LockOperation operation,
                                           ResponseWriter responseWriter);

  protected void writeResponse(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry) {
    recordMetric(status);
    ApiLockCodec.writeLockResponse(ctx, status, requestId, entry);
  }

  protected void writeBadRequest(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_BAD_REQUEST);
    ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
  }

  protected void writeUnavailable(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_UNAVAILABLE);
    ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
  }

  protected void writeNotFound(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_NOT_FOUND);
    ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
  }

  protected void writeLockHeld(ChannelHandlerContext ctx, int requestId, LockEntry entry) {
    recordMetric(ApiProtocol.ST_LOCK_HELD);
    ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_LOCK_HELD, requestId, entry);
  }

  protected abstract void recordMetric(byte status);

  /**
   * Результат lock-операции.
   */
  public record LockOperationResult(boolean applied, byte status, LockEntry entry, boolean deleted) {
    public LockOperationResult(boolean applied, byte status, LockEntry entry) {
      this(applied, status, entry, false);
    }
  }

  /**
   * Функциональный интерфейс для выполнения операции на primary.
   */
  @FunctionalInterface
  public interface LockOperation {
    LockOperationResult execute();
  }

  /**
   * Функциональный интерфейс для формирования ответа.
   */
  @FunctionalInterface
  public interface ResponseWriter {
    void write(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry);
  }
}
