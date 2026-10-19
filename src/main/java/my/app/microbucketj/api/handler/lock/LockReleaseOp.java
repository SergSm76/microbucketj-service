package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_RELEASE_REQ;
import static my.app.microbucketj.utils.Hashing.fnv1a64;

/**
 * Обработчик операции освобождения блокировки (LOCK_RELEASE).
 *
 * Семантика:
 * - Если текущий узел — primary для данного lock → выполнить локально + реплицировать
 * - Иначе → forward на primary через TokenPeerClient
 *
 * Блокировка освобождается только если:
 * - Запись существует
 * - Владелец совпадает с lockedBy в запросе (или блокировка уже истекла)
 */
public final class LockReleaseOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockReleaseOp.class);

  private final ClusterPeerClient clusterPeers;

  public LockReleaseOp(BooleanSupplier isLeader,
                       MembershipRuntime membership,
                       LockRegistry locks,
                       TokenPeerClient tokenPeers,
                       ReplicationService replication,
                       ClusterPeerClient clusterPeers) {
    super(isLeader, membership, locks, tokenPeers, replication);
    this.clusterPeers = Objects.requireNonNull(clusterPeers, "clusterPeers");
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_RELEASE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_RELEASE_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);
    String lockedBy = (name == null) ? null : ApiLockCodec.readLockedBy(in);

    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank()) {
      recordMetric(ApiProtocol.ST_BAD_REQUEST);
      ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    final String lockName = name;
    final String lockOwner = lockedBy;

    // Вычисляем hash для маршрутизации
    long lockHash = fnv1a64(lockName);

    // Проверяем, можем ли мы обработать запрос
    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      recordMetric(ApiProtocol.ST_UNAVAILABLE);
      ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (primary.id().equals(view.self().id())) {
      // Текущий узел — primary, выполнить локально
      LockRegistry.LockResult res;
      try {
        res = locks.release(lockName, lockOwner);
      } catch (IllegalArgumentException e) {
        recordMetric(ApiProtocol.ST_BAD_REQUEST);
        ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
        return;
      }

      if (!res.applied()) {
        if (res.current() == null) {
          recordMetric(ApiProtocol.ST_NOT_FOUND);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId, null);
          LOG.debug("Операция lock-release отклонена: lock={} не существует", lockName);
        } else {
          recordMetric(ApiProtocol.ST_LOCK_HELD);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_LOCK_HELD, requestId, res.current());
          LOG.debug("Операция lock-release отклонена: lock={} heldBy={} until={}",
              lockName, res.current().lockedBy(), res.current().lockUntilMs());
        }
        return;
      }

      LOG.info("Операция lock-release применена: lock={} by={}", lockName, lockOwner);

      // Репликация удаления с ожиданием MIN_ACKS
      var replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, lockName, null, true);
      if (replF.isDone()) {
        boolean ok = replF.getNow(false);
        if (!ok) {
          recordMetric(ApiProtocol.ST_UNAVAILABLE);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, res.current());
        } else {
          recordMetric(ApiProtocol.ST_OK);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_OK, requestId, res.current());
        }
        return;
      }

      final LockEntry releasedEntry = res.current();
      replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
        if (err != null || !ok) {
          recordMetric(ApiProtocol.ST_UNAVAILABLE);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, releasedEntry);
        } else {
          recordMetric(ApiProtocol.ST_OK);
          ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_OK, requestId, releasedEntry);
        }
      }));
      return;
    }

    // Forward на primary
    tokenPeers.forwardLockRelease(primary, lockName, lockOwner)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            recordMetric(ApiProtocol.ST_UNAVAILABLE);
            ApiLockCodec.writeLockResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }
          recordMetric(res.status());
          ApiLockCodec.writeLockResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void forwardToPrimary(ChannelHandlerContext ctx, int clientRequestId, Node primary,
                                  String lockName, LockOperation operation, ResponseWriter responseWriter) {
    // Не используется — реализовано inline в handle()
    throw new UnsupportedOperationException("Forward is handled inline in handle()");
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics metrics = MetricsHolder.get();
    if (metrics != null) metrics.recordLockRelease(status);
  }
}
