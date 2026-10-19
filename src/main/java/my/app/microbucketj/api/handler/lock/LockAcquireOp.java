package my.app.microbucketj.api.handler.lock;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.observability.MetricsHolder;
import my.app.microbucketj.observability.PrometheusMetrics;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.api.ApiLockCodec;
import my.app.microbucketj.token.TokenPeerClient;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_ACQUIRE_REQ;

/**
 * LOCK_ACQUIRE — захват блокировки.
 *
 * Путь выполнения:
 *   1. Если этот узел является единственным в кластере и не лидер → ST_UNAVAILABLE.
 *   2. Если этот узел — primary для данной блокировки → выполняем локально
 *      через {@link LockExecutor#executeOnPrimary} и реплицируем.
 *   3. Иначе → форвардим на primary через {@link TokenPeerClient#forwardLockAcquire}.
 */
public final class LockAcquireOp extends AbstractLockCommon {

  private final LockExecutor executor;
  private final TokenPeerClient tokenPeers;

  public LockAcquireOp(LockExecutor executor, TokenPeerClient tokenPeers) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.tokenPeers = Objects.requireNonNull(tokenPeers, "tokenPeers");
  }

  @Override
  public byte opcode() {
    return ApiProtocol.OP_LOCK_ACQUIRE;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    if (in.readableBytes() < MIN_LOCK_ACQUIRE_REQ) return;
    int requestId = ApiLockCodec.readRequestId(in);

    String name = ApiLockCodec.readLockName(in);
    long lockUntilMs = (name == null || in.readableBytes() < 8) ? 0L : ApiLockCodec.readLockUntil(in);
    String lockedBy = (name == null) ? null : ApiLockCodec.readLockedBy(in);

    // Валидация входных данных
    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank() || lockUntilMs <= 0) {
      writeBadRequest(ctx, requestId);
      return;
    }

    // Единственный узел в кластере, но не лидер → операция невозможна
    if (executor.isSingleNodeNonLeader()) {
      writeUnavailable(ctx, requestId);
      return;
    }

    // Мы — primary: выполняем локально + реплицируем
    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> locks.acquire(name, lockUntilMs, lockedBy),
              ApiProtocol.ST_OK,
              false)  // acquire записывает, а не удаляет
          .whenComplete((r, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              writeUnavailable(ctx, requestId);
              return;
            }
            writeLockResponse(ctx, r.status(), requestId, r.current());
          }));
      return;
    }

    // Мы не primary: форвардим на первичный узел
    Node primary = executor.primaryFor(name);
    tokenPeers.forwardLockAcquire(primary, name, lockUntilMs, lockedBy)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null || res == null || res.unavailable()) {
            writeUnavailable(ctx, requestId);
            return;
          }
          writeLockResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockAcquire(status);
  }
}
