package my.app.microbucketj.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Small, fast opcode-based dispatcher.
 * <p>
 * - reads opcode byte
 * - delegates to a registered {@link OpcodeHandler}
 * - unknown opcodes are ignored by default
 */
public abstract class AbstractOpcodeDispatcher extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOpcodeDispatcher.class);

  private final OpcodeHandler[] table = new OpcodeHandler[256];

  protected AbstractOpcodeDispatcher(OpcodeHandler... handlers) {
    super(true);
    Arrays.fill(table, null);
    OpcodeHandler[] hs = handlers == null ? new OpcodeHandler[0] : handlers;
    for (OpcodeHandler h : hs) {
      if (h == null) continue;
      int idx = h.opcode() & 0xFF;
      OpcodeHandler prev = table[idx];
      if (prev != null) {
        throw new IllegalArgumentException("Duplicate opcode handler for op=" + (byte) idx + ": " + prev + " and " + h);
      }
      table[idx] = h;
    }
  }

  @Override
  protected final void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
    if (in == null || !in.isReadable()) return;
    byte op = in.readByte();
    OpcodeHandler opcodeHandler = table[op & 0xFF];
    if (opcodeHandler == null) {
      onUnknownOpcode(ctx, op, in);
      return;
    }
    opcodeHandler.handle(ctx, in);
  }

  /**
   * Default: ignore unknown opcode.
   */
  protected void onUnknownOpcode(ChannelHandlerContext ctx, byte op, ByteBuf payload) {
    // ignore
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Ошибка в обработчике, закрытие канала {}", ctx.channel(), cause);
    ctx.close();
  }
}
