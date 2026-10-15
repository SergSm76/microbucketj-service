package my.app.microbucketj.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * A single-frame opcode handler.
 *
 * The dispatcher consumes the opcode byte and passes the remaining payload buffer to the handler.
 */
public interface OpcodeHandler {

  byte opcode();

  /**
   * Handle message payload (opcode is already consumed).
   */
  void handle(ChannelHandlerContext ctx, ByteBuf payload);
}
