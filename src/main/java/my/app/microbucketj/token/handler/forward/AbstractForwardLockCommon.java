package my.app.microbucketj.token.handler.forward;

import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.lock.LockExecutor;
import my.app.microbucketj.protocol.ApiProtocol;
import my.app.microbucketj.protocol.codec.token.TokenForwardLockCodec;

import java.util.Objects;

/**
 * Базовый класс для forward lock-опов (полученных по internal-каналу от другого узла).
 *
 * Логика: запрос уже пришёл на этот узел (он является или должен являться primary
 * для данной блокировки). Задача — выполнить операцию локально, реплицировать
 * и отправить ответ через internal-канал.
 *
 * Наследники реализуют {@code opcode()} и {@code handle()} из OpcodeHandler.
 */
public abstract class AbstractForwardLockCommon {

  protected final LockExecutor executor;

  protected AbstractForwardLockCommon(LockExecutor executor) {
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  /**
   * Выполняет операцию на этом узле и отправляет ответ через internal-канал.
   *
   * @param ctx           канал для ответа
   * @param requestId     ID запроса (для корреляции на клиенте)
   * @param lockName      имя блокировки
   * @param op            локальная операция над LockRegistry
   * @param successStatus статус при успешном применении (обычно ST_OK)
   * @param deleted       true если операция удаляет запись (release, delete)
   */
  protected void executeAndRespond(ChannelHandlerContext ctx,
                                   int requestId,
                                   String lockName,
                                   LockExecutor.Operation op,
                                   byte successStatus,
                                   boolean deleted) {
    if (lockName == null || lockName.isBlank()) {
      writeFwdResp(ctx, ApiProtocol.ST_BAD_REQUEST, requestId, null);
      return;
    }

    executor.executeOnPrimary(lockName, op, successStatus, deleted)
        .whenComplete((r, err) -> ctx.executor().execute(() -> {
          if (err != null) {
            writeFwdResp(ctx, ApiProtocol.ST_UNAVAILABLE, requestId, null);
            return;
          }
          writeFwdResp(ctx, r.status(), requestId, r.current());
        }));
  }

  /** Отправляет ответ через internal-канал (с фреймингом). */
  protected void writeFwdResp(ChannelHandlerContext ctx, byte status, int requestId, LockEntry entry) {
    TokenForwardLockCodec.writeFramedResponse(ctx, requestId, status, entry);
  }
}
