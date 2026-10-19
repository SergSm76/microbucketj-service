package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.cluster.ClusterPeerClient;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockEntry;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_UPDATE_REQ;
import static my.app.microbucketj.utils.Hashing.fnv1a64;

public final class LockUpdateOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockUpdateOp.class);

  private final ClusterPeerClient clusterPeers;

  public LockUpdateOp(BooleanSupplier isLeader,
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
    return ApiProtocol.OP_LOCK_UPDATE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_UPDATE_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);
    long lockUntilMs = (name == null || in.readableBytes() < 8) ? 0L : ApiLockCodec.readLockUntil(in);
    String lockedBy = (name == null) ? null : ApiLockCodec.readLockedBy(in);

    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank() || lockUntilMs <= 0) {
      writeBadRequest(ctx, requestId);
      return;
    }

    final String lockName = name;
    final long lockUntil = lockUntilMs;
    final String lockOwner = lockedBy;

    long lockHash = fnv1a64(lockName);
    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      writeUnavailable(ctx, requestId);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (primary.id().equals(view.self().id())) {
      // Локальное выполнение
      LockRegistry.LockResult res;
      try {
        res = locks.update(lockName, lockUntil, lockOwner);
      } catch (IllegalArgumentException e) {
        writeBadRequest(ctx, requestId);
        return;
      }

      if (!res.applied()) {
        if (res.current() == null) {
          writeNotFound(ctx, requestId);
        } else {
          writeLockHeld(ctx, requestId, res.current());
        }
        return;
      }

      LOG.info("Операция lock-update применена: lock={} by={} until={}", lockName, lockOwner, lockUntil);

      // Репликация
      var replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, res.current(), false);
      if (replF.isDone()) {
        if (!replF.getNow(false)) {
          writeUnavailable(ctx, requestId);
        } else {
          writeResponse(ctx, ApiProtocol.ST_OK, requestId, res.current());
        }
        return;
      }

      replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
        if (err != null || !ok) {
          writeUnavailable(ctx, requestId);
        } else {
          writeResponse(ctx, ApiProtocol.ST_OK, requestId, res.current());
        }
      }));
      return;
    }

    // Forward на primary
    tokenPeers.forwardLockUpdate(primary, lockName, lockUntil, lockOwner)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeUnavailable(ctx, requestId);
            return;
          }
          writeResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void forwardToPrimary(ChannelHandlerContext ctx, int clientRequestId, Node primary,
                                  String lockName, LockOperation operation, ResponseWriter responseWriter) {
    // Не используется — реализовано inline в handle
  }

  @Override
  protected void recordMetric(byte status) {
    var m = MetricsHolder.get();
    if (m != null) m.recordLockUpdate(status);
  }
}
