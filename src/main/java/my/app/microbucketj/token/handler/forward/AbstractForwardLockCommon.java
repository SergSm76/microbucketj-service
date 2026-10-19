package my.app.microbucketj.token.handler.forward;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.model.ClusterView;
import my.app.microbucketj.cluster.model.MembershipRuntime;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockRegistry;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.utils.Hashing;
import java.util.Objects;

public abstract class AbstractForwardLockCommon {
  protected final MembershipRuntime membership;
  protected final LockRegistry locks;
  protected final ReplicationService replication;

  protected AbstractForwardLockCommon(MembershipRuntime membership, LockRegistry locks, ReplicationService replication) {
    this.membership = Objects.requireNonNull(membership);
    this.locks = Objects.requireNonNull(locks);
    this.replication = Objects.requireNonNull(replication);
  }

  private enum InternalLockOpcode { ACQUIRE, RELEASE, UPDATE, DELETE }

  protected void runAcquire(ChannelHandlerContext ctx, int requestId, String lockName, long lockUntilMs, String lockedBy) {
    runInternal(ctx, requestId, lockName, lockUntilMs, lockedBy, InternalLockOpcode.ACQUIRE);
  }

  protected void runRelease(ChannelHandlerContext ctx, int requestId, String lockName, String lockedBy) {
    runInternal(ctx, requestId, lockName, 0L, lockedBy, InternalLockOpcode.RELEASE);
  }

  protected void runUpdate(ChannelHandlerContext ctx, int requestId, String lockName, long lockUntilMs, String lockedBy) {
    runInternal(ctx, requestId, lockName, lockUntilMs, lockedBy, InternalLockOpcode.UPDATE);
  }

  protected void runDelete(ChannelHandlerContext ctx, int requestId, String lockName) {
    runInternal(ctx, requestId, lockName, 0L, null, InternalLockOpcode.DELETE);
  }

  private void runInternal(ChannelHandlerContext ctx, int requestId, String lockName,
                           long lockUntilMs, String lockedBy, InternalLockOpcode opcode) {
    if (lockName == null || lockName.isBlank()) { writeFwdResp(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null); return; }

    long lockHash = Hashing.fnv1a64(lockName);
    var snap = membership.snapshot();
    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (!primary.id().equals(view.self().id())) {
      writeFwdResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      return;
    }

    LockRegistry.LockResult res;
    boolean deleted = false;
    try {
      switch (opcode) {
        case ACQUIRE: res = locks.acquire(lockName, lockUntilMs, lockedBy); break;
        case RELEASE: res = locks.release(lockName, lockedBy); break;
        case UPDATE: res = locks.update(lockName, lockUntilMs, lockedBy); break;
        case DELETE:
          boolean ok = locks.delete(lockName);
          res = new LockRegistry.LockResult(ok, null);
          deleted = true;
          break;
        default: writeFwdResp(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null); return;
      }
    } catch (Exception e) {
      writeFwdResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null); return;
    }

    if (!res.applied()) {
      byte status = (res.current() == null) ? ApiProtocol.ST_NOT_FOUND : ApiProtocol.ST_LOCK_HELD;
      writeFwdResp(ctx, status, requestId, res.current());
      return;
    }

    var replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, lockName, res.current(), deleted);
    if (replF.isDone()) {
      boolean ok = replF.getNow(false);
      writeFwdResp(ctx, ok ? ApiProtocol.ST_OK : ApiProtocol.ST_UNAVAILABLE, requestId, res.current());
      return;
    }

    replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
      if (err != null || !ok) writeFwdResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
      else writeFwdResp(ctx, ApiProtocol.ST_OK, requestId, res.current());
    }));
  }

  protected void writeFwdResp(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry) {
    TokenForwardLockCodec.writeFramedResponse(ctx, requestId, status, entry);
  }
}
