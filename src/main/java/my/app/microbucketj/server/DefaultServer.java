package my.app.microbucketj.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class DefaultServer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultServer.class);

  private final EventLoopGroup boss;
  private final EventLoopGroup workers;
  private final Channel server;

  public DefaultServer(String prefix,
                       String bindHost,
                       int port,
                       int ioThreads,
                       int maxFrameSize,
                       SslContext sslContext,
                       Supplier<? extends ChannelHandler> handlerFactory) {

    boolean epoll = Epoll.isAvailable();

    IoHandlerFactory ioFactory = epoll
        ? EpollIoHandler.newFactory()
        : NioIoHandler.newFactory();

    this.boss = new MultiThreadIoEventLoopGroup(1,
        new DefaultThreadFactory("mbj-b-" + prefix + "-io", 10),
        ioFactory);
    this.workers = new MultiThreadIoEventLoopGroup(ioThreads,
        new DefaultThreadFactory("mbj-w-" + prefix + "-io", 10),
        ioFactory);

    Class<? extends ServerSocketChannel> srv = epoll
        ? EpollServerSocketChannel.class
        : NioServerSocketChannel.class;

    ServerBootstrap b = new ServerBootstrap()
        .group(boss, workers)
        .channel(srv)
        .option(ChannelOption.SO_BACKLOG, 1024)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(32 * 1024, 256 * 1024))
        .childHandler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(Channel ch) {
            ChannelPipeline p = ch.pipeline();
            if (sslContext != null) {
              p.addLast(sslContext.newHandler(ch.alloc()));
            }
            p.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
            p.addLast(handlerFactory.get());
          }
        });

    this.server = b.bind(bindHost, port).syncUninterruptibly().channel();
    LOG.info("Сервер {} привязан к {}:{} (epoll={})", prefix, bindHost, port, epoll);
  }

  @Override
  public void close() {
    try {
      LOG.info("Сервер закрыт: {}", server.localAddress());
      server.close().syncUninterruptibly();
    } finally {
      boss.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      workers.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }
  }
}
