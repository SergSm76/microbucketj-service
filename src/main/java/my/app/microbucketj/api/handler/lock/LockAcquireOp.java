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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static my.app.microbucketj.protocol.ApiProtocol.MIN_LOCK_ACQUIRE_REQ;

/**
 * LOCK_ACQUIRE — захват блокировки.
 * <p>
 * Путь выполнения:
 * 1. Если этот узел является единственным в кластере и не лидер → ST_UNAVAILABLE.
 * 2. Если этот узел — primary для данной блокировки → выполняем локально
 * через {@link LockExecutor#executeOnPrimary} и реплицируем.
 * 3. Иначе → форвардим на primary через {@link TokenPeerClient#forwardLockAcquire}.
 */
public final class LockAcquireOp extends AbstractLockCommon {

  private static final Logger LOG = LoggerFactory.getLogger(LockAcquireOp.class);

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

    LOG.debug("Получен запрос LOCK_ACQUIRE: reqId={} name={} until={} owner={}",
        requestId, name, lockUntilMs, lockedBy);

    // Валидация входных данных
    if (name == null || name.isBlank() || lockedBy == null || lockedBy.isBlank() || lockUntilMs <= 0) {
      LOG.debug("Некорректные параметры в запросе LOCK_ACQUIRE: reqId={}", requestId);
      writeBadRequest(ctx, requestId);
      return;
    }

    // Единственный узел в кластере, но не лидер → операция невозможна
    if (executor.isSingleNodeNonLeader()) {
      LOG.debug("Отказ в LOCK_ACQUIRE (reqId={}): узел не лидер в одиночном режиме", requestId);
      writeUnavailable(ctx, requestId);
      return;
    }

    // Мы — primary: выполняем локально + реплицируем
    if (executor.isSelfPrimary(name)) {
      executor.executeOnPrimary(name,
              locks -> locks.acquire(name, lockUntilMs, lockedBy),
              ApiProtocol.ST_OK,
              false)  // acquire записывает, а не удаляет
          .whenComplete((res, err) -> ctx.executor().execute(() -> {
            if (err != null) {
              LOG.debug("Ошибка при репликации LOCK_ACQUIRE: reqId={} lock={} error={}", requestId, name, err.getMessage());
              writeUnavailable(ctx, requestId);
              return;
            }

            LOG.debug("Успешная репликация LOCK_ACQUIRE: reqId={} lock={} status={}", requestId, name, res.status());
            writeLockResponse(ctx, res.status(), requestId, res.current());
          }));
      return;
    }

    // Мы не primary: форвардим на первичный узел
    Node primary = executor.primaryFor(name);
    LOG.debug("Перенаправление LOCK_ACQUIRE на primary: reqId={} lock={} primary={}", requestId, name, primary.id());
    tokenPeers.forwardLockAcquire(primary, name, lockUntilMs, lockedBy)
        .whenComplete((res, err) -> ctx.executor().execute(() -> {
          if (err != null) {
            LOG.debug("Ошибка при перенаправлении LOCK_ACQUIRE: reqId={} lock={} error={}", requestId, name, err.getMessage());
            writeUnavailable(ctx, requestId);
            return;
          }

          if (res == null) {
            LOG.debug("Получен null-ответ при перенаправлении LOCK_ACQUIRE: reqId={} lock={}", requestId, name);
            writeUnavailable(ctx, requestId);
            return;
          }

          if (res.unavailable()) {
            LOG.debug("Primary вернул UNAVAILABLE при перенаправлении LOCK_ACQUIRE: reqId={} lock={} primary={}",
                requestId, name, primary.id());
            writeUnavailable(ctx, requestId);
            return;
          }

          LOG.debug("Успешное перенаправление LOCK_ACQUIRE: reqId={} lock={} status={} primary={}",
              requestId, name, res.status(), primary.id());
          writeLockResponse(ctx, res.status(), requestId, res.entry());
        }));
  }

  @Override
  protected void recordMetric(byte status) {
    PrometheusMetrics m = MetricsHolder.get();
    if (m != null) m.recordLockAcquire(status);
  }
}
