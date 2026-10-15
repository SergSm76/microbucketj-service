package my.app.microbucketj.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.internal.InternalHelloCodec;

import java.util.function.Supplier;

public final class UnifiedInternalServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(UnifiedInternalServerHandler.class);

  private final Supplier<? extends ChannelHandler> clusterHandlerFactory;
  private final Supplier<? extends ChannelHandler> tokenHandlerFactory;

  private boolean decided;

  public UnifiedInternalServerHandler(Supplier<? extends ChannelHandler> clusterHandlerFactory,
                                      Supplier<? extends ChannelHandler> tokenHandlerFactory) {
    super(true);
    this.clusterHandlerFactory = clusterHandlerFactory;
    this.tokenHandlerFactory = tokenHandlerFactory;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
    if (decided) {
      return;
    }
    if (!in.isReadable()) {
      ctx.close();
      return;
    }

    final InternalHelloCodec.HelloData hello;
    try {
      hello = InternalHelloCodec.read(in);
    } catch (Exception e) {
      LOG.debug("InternalHelloCodec: bad HELLO из {}", ctx.channel().remoteAddress(), e);
      ctx.close();
      return;
    }

    byte role = hello.role();
    byte protocolVersion = hello.protocolVersion();
    String nodeId = hello.nodeId();

    Supplier<? extends ChannelHandler> factory = switch (role) {
      case InternalProtocol.ROLE_TOKEN -> tokenHandlerFactory;
      case InternalProtocol.ROLE_CLUSTER -> clusterHandlerFactory;
      default -> null;
    };

    if (factory == null) {
      LOG.debug("InternalHelloCodec: неизвестная роль {} из {} nodeId={} version={}", role, ctx.channel().remoteAddress(), nodeId, protocolVersion);
      ctx.close();
      return;
    }

    ChannelHandler next = factory.get();
    if (next == null) {
      ctx.close();
      return;
    }

    decided = true;
    ChannelPipeline p = ctx.pipeline();
    p.replace(this, role == InternalProtocol.ROLE_TOKEN ? "token" : "cluster", next);
    LOG.debug("InternalHelloCodec: роль {} выбрана для {} nodeId={} version={} versionedHello={}",
        role == InternalProtocol.ROLE_TOKEN ? "TOKEN" : "CLUSTER",
        ctx.channel().remoteAddress(),
        nodeId,
        protocolVersion,
        hello.versioned());
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.debug("InternalHelloCodec: ошибка, закрытие канала {}", ctx.channel(), cause);
    ctx.close();
  }
}
