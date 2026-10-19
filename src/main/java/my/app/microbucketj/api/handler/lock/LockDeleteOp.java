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
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.replication.ReplicationService;
import my.app.microbucketj.token.TokenPeerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BooleanSupplier;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_GET_REQ;
import static my.app.microbucketj.utils.Hashing.fnv1a64;

public final class LockDeleteOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockDeleteOp.class);

  private final ClusterPeerClient clusterPeers;

  public LockDeleteOp(BooleanSupplier isLeader,
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
    return ApiProtocol.OP_LOCK_DELETE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_GET_REQ) return; // MIN_LOCK_GET_REQ = 4 + 1 (requestId + nameLen)
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);

    if (name == null || name.isBlank()) {
      writeSimpleBadRequest(ctx, requestId);
      return;
    }

    final String lockName = name;

    long lockHash = fnv1a64(lockName);
    var snap = membership.snapshot();
    if (!isLeader.getAsBoolean() && snap.nodes().size() <= 1) {
      writeSimpleUnavailable(ctx, requestId);
      return;
    }

    ClusterView view = snap.view();
    Node primary = view.pickPrimary(lockHash);

    if (primary.id().equals(view.self().id())) {
      // Локальное выполнение
      boolean deleted = locks.delete(lockName);

      if (!deleted) {
        writeSimpleNotFound(ctx, requestId);
        return;
      }

      LOG.info("Операция lock-delete применена: lock={}", lockName);

      // Репликация удаления
      var replF = replication.replicateLockMutation(view, snap.epoch(), lockHash, null, true);
      if (replF.isDone()) {
        if (!replF.getNow(false)) {
          writeSimpleUnavailable(ctx, requestId);
        } else {
          writeSimpleOk(ctx, requestId);
        }
        return;
      }

      replF.whenComplete((ok, err) -> ctx.executor().execute(() -> {
        if (err != null || !ok) {
          writeSimpleUnavailable(ctx, requestId);
        } else {
          writeSimpleOk(ctx, requestId);
        }
      }));
      return;
    }

    // Forward на primary
    tokenPeers.forwardLockDelete(primary, lockName)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeSimpleUnavailable(ctx, requestId);
            return;
          }
          if (res.status() == ApiProtocol.ST_NOT_FOUND) {
            writeSimpleNotFound(ctx, requestId);
          } else if (res.status() == ApiProtocol.ST_OK) {
            writeSimpleOk(ctx, requestId);
          } else {
            writeSimpleUnavailable(ctx, requestId);
          }
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
    if (m != null) m.recordLockDelete(status);
  }

  // Простые ответы для delete (без LockEntry)
  private void writeSimpleOk(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_OK);
    ApiLockCodec.writeSimpleResponse(ctx, ApiProtocol.ST_OK, requestId);
  }

  private void writeSimpleNotFound(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_NOT_FOUND);
    ApiLockCodec.writeSimpleResponse(ctx, ApiProtocol.ST_NOT_FOUND, requestId);
  }

  private void writeSimpleUnavailable(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_UNAVAILABLE);
    ApiLockCodec.writeSimpleResponse(ctx, ApiProtocol.ST_UNAVAILABLE, requestId);
  }

  private void writeSimpleBadRequest(ChannelHandlerContext ctx, int requestId) {
    recordMetric(ApiProtocol.ST_BAD_REQUEST);
    ApiLockCodec.writeSimpleResponse(ctx, ApiProtocol.ST_BAD_REQUEST, requestId);
  }
}
