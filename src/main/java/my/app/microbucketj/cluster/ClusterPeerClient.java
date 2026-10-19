package my.app.microbucketj.cluster;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import my.app.microbucketj.api.model.LockEntry;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.config.BucketSpec;
import my.app.microbucketj.protocol.ClusterProtocol;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.codec.cluster.ClusterBucketUpsertCodec;
import my.app.microbucketj.protocol.codec.cluster.ClusterJoinCodec;
import my.app.microbucketj.protocol.codec.cluster.ClusterLockCodec;
import my.app.microbucketj.protocol.codec.cluster.ClusterMembershipCodec;
import my.app.microbucketj.protocol.codec.cluster.ClusterPingPongCodec;
import my.app.microbucketj.protocol.codec.internal.InternalHelloCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class ClusterPeerClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterPeerClient.class);

  private final EventLoopGroup group;
  private final Bootstrap bootstrap;
  private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> connectInFlight = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> reconnectAllowedAtNanos = new ConcurrentHashMap<>();
  private final long connectBackoffNanos;
  private final String selfId;

  public ClusterPeerClient(int ioThreads,
                           int maxFrameSize,
                           long heartbeatIntervalMs,
                           long heartbeatTimeoutMs,
                           String selfId,
                           long connectBackoffMs) {
    boolean epoll = Epoll.isAvailable();
    this.selfId = selfId == null ? "" : selfId;
    this.connectBackoffNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(100, connectBackoffMs));

    IoHandlerFactory ioFactory = epoll ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
    this.group = new MultiThreadIoEventLoopGroup(
        Math.max(1, ioThreads / 2),
        new DefaultThreadFactory("mbj-crc-io", 10),
        ioFactory
    );

    Class<? extends Channel> ch = epoll ? EpollSocketChannel.class : NioSocketChannel.class;

    long hbI = Math.max(100, heartbeatIntervalMs);
    long hbT = Math.max(hbI * 2, heartbeatTimeoutMs);

    this.bootstrap = new Bootstrap()
        .group(group)
        .channel(ch)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .handler(new ChannelInitializer<>() {
          @Override
          protected void initChannel(Channel c) {
            ChannelPipeline p = c.pipeline();
            p.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
            p.addLast(new LengthFieldPrepender(4));
            p.addLast(new IdleStateHandler(hbT, hbI, 0, TimeUnit.MILLISECONDS));
            p.addLast(new ClientHandler());
          }
        });
  }

  private static ByteBuf buildMembershipFrame(Channel channel, List<Node> nodes, int epoch) {
    ByteBuf out = channel.alloc().ioBuffer(128 + nodes.size() * 64);
    ClusterMembershipCodec.writePayload(out, nodes, epoch);
    return out;
  }

  private static ByteBuf buildBucketUpsertFrame(Channel channel, BucketSpec spec) {
    ByteBuf out = channel.alloc().ioBuffer(256);
    ClusterBucketUpsertCodec.writePayload(out, spec);
    return out;
  }

  private static ByteBuf buildLockUpsertFrame(Channel channel, LockEntry entry) {
    ByteBuf out = channel.alloc().ioBuffer(128);
    ClusterLockCodec.writeUpsertPayload(out, entry);   // OP_LOCK_UPSERT + entry
    return out;
  }

  private static ByteBuf buildLockDeleteFrame(Channel channel, String name) {
    ByteBuf out = channel.alloc().ioBuffer(96);
    ClusterLockCodec.writeDeletePayload(out, name);    // OP_LOCK_DELETE + name
    return out;
  }

  public void connectAll(List<Node> nodes, Node self) {
    for (Node node : nodes) {
      if (node.id().equals(self.id())) continue;
      connect(node);
    }
  }

  public void connect(Node node) {
    String key = node.clusterAddrKey();
    Channel existing = channels.get(key);
    if (existing != null && existing.isActive()) return;
    Long reconnectAt = reconnectAllowedAtNanos.get(key);
    long now = System.nanoTime();
    if (reconnectAt != null && now < reconnectAt) return;
    if (connectInFlight.putIfAbsent(key, Boolean.TRUE) != null) return;

    bootstrap.connect(node.host(), node.clusterPort()).addListener((ChannelFutureListener) future -> {
      connectInFlight.remove(key);
      if (!future.isSuccess()) {
        reconnectAllowedAtNanos.put(key, System.nanoTime() + connectBackoffNanos);
        LOG.debug("Не удалось подключиться к кластеру {}:{}", node.host(), node.clusterPort(), future.cause());
        return;
      }
      Channel channel = future.channel();
      ByteBuf hello = channel.alloc().ioBuffer(64);
      InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
      channel.writeAndFlush(hello);
      channels.put(key, channel);
      reconnectAllowedAtNanos.remove(key);
      channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> {
        channels.remove(key, channel);
        reconnectAllowedAtNanos.put(key, System.nanoTime() + connectBackoffNanos);
      });
    });
  }

  public boolean isConnected(Node node) {
    if (node == null) return false;
    Channel ch = channels.get(node.clusterAddrKey());
    return ch != null && ch.isActive();
  }

  public void sendJoin(String host, int port, Node self) {
    bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
      if (!future.isSuccess()) {
        LOG.debug("Не удалось отправить join к кластеру {}:{}", host, port, future.cause());
        return;
      }
      Channel channel = future.channel();
      ByteBuf hello = channel.alloc().ioBuffer(64);
      InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
      ByteBuf out = channel.alloc().ioBuffer(256);
      ClusterJoinCodec.writePayload(out, self);
      channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
        if (!helloFuture.isSuccess()) {
          channel.close();
          return;
        }
        channel.writeAndFlush(out).addListener(ChannelFutureListener.CLOSE);
      });
    });
  }

  public void broadcastMembership(List<Node> nodes, int epoch, Node self) {
    for (Node node : nodes) {
      if (node.id().equals(self.id())) continue;
      Channel ch = channels.get(node.clusterAddrKey());
      if (ch != null && ch.isActive()) {
        ch.writeAndFlush(buildMembershipFrame(ch, nodes, epoch));
        continue;
      }
      bootstrap.connect(node.host(), node.clusterPort()).addListener((ChannelFutureListener) future -> {
        if (!future.isSuccess()) return;
        Channel channel = future.channel();
        channels.put(node.clusterAddrKey(), channel);
        channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> channels.remove(node.clusterAddrKey(), channel));
        ByteBuf hello = channel.alloc().ioBuffer(64);
        InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
        channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
          if (!helloFuture.isSuccess()) {
            channel.close();
            return;
          }
          channel.writeAndFlush(buildMembershipFrame(channel, nodes, epoch));
        });
      });
    }
  }

  public void sendMembershipTo(Node target, List<Node> nodes, int epoch) {
    if (target == null) return;
    Channel ch = channels.get(target.clusterAddrKey());
    if (ch != null && ch.isActive()) {
      ch.writeAndFlush(buildMembershipFrame(ch, nodes, epoch));
      return;
    }
    bootstrap.connect(target.host(), target.clusterPort()).addListener((ChannelFutureListener) future -> {
      if (!future.isSuccess()) return;
      Channel channel = future.channel();
      channels.put(target.clusterAddrKey(), channel);
      channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> channels.remove(target.clusterAddrKey(), channel));
      ByteBuf hello = channel.alloc().ioBuffer(64);
      InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
      channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
        if (!helloFuture.isSuccess()) {
          channel.close();
          return;
        }
        channel.writeAndFlush(buildMembershipFrame(channel, nodes, epoch));
      });
    });
  }

  public void broadcastBucketUpsert(List<Node> nodes, Node self, BucketSpec spec) {
    if (spec == null || spec.name() == null || spec.name().isBlank()) return;
    for (Node node : nodes) {
      if (node.id().equals(self.id())) continue;
      Channel ch = channels.get(node.clusterAddrKey());
      if (ch != null && ch.isActive()) {
        ch.writeAndFlush(buildBucketUpsertFrame(ch, spec));
        continue;
      }
      bootstrap.connect(node.host(), node.clusterPort()).addListener((ChannelFutureListener) future -> {
        if (!future.isSuccess()) return;
        Channel channel = future.channel();
        channels.put(node.clusterAddrKey(), channel);
        channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> channels.remove(node.clusterAddrKey(), channel));
        ByteBuf hello = channel.alloc().ioBuffer(64);
        InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
        channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
          if (!helloFuture.isSuccess()) {
            channel.close();
            return;
          }
          channel.writeAndFlush(buildBucketUpsertFrame(channel, spec));
        });
      });
    }
  }

  public void broadcastLockUpsert(List<Node> nodes, Node self, LockEntry entry) {
    // ... тело один-в-один как broadcastBucketUpsert, но с buildLockUpsertFrame
    if (entry == null || entry.name() == null || entry.name().isBlank()) return;
    for (Node node : nodes) {
      if (node.id().equals(self.id())) continue;
      Channel ch = channels.get(node.clusterAddrKey());
      if (ch != null && ch.isActive()) {
        ch.writeAndFlush(buildLockUpsertFrame(ch, entry));
        continue;
      }
      bootstrap.connect(node.host(), node.clusterPort()).addListener((ChannelFutureListener) future -> {
        if (!future.isSuccess()) return;
        Channel channel = future.channel();
        channels.put(node.clusterAddrKey(), channel);
        channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> channels.remove(node.clusterAddrKey(), channel));
        ByteBuf hello = channel.alloc().ioBuffer(64);
        InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
        channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
          if (!helloFuture.isSuccess()) {
            channel.close();
            return;
          }
          channel.writeAndFlush(buildLockUpsertFrame(channel, entry));
        });
      });
    }
  }

  public void broadcastLockDelete(List<Node> nodes, Node self, String name) {
    // ... тело один-в-один как broadcastBucketUpsert, но с buildLockDeleteFrame
    if (name == null || name.isBlank()) return;
    for (Node node : nodes) {
      if (node.id().equals(self.id())) continue;
      Channel ch = channels.get(node.clusterAddrKey());
      if (ch != null && ch.isActive()) {
        ch.writeAndFlush(buildLockDeleteFrame(ch, name));
        continue;
      }
      bootstrap.connect(node.host(), node.clusterPort()).addListener((ChannelFutureListener) future -> {
        if (!future.isSuccess()) return;
        Channel channel = future.channel();
        channels.put(node.clusterAddrKey(), channel);
        channel.closeFuture().addListener((ChannelFutureListener) closeFuture -> channels.remove(node.clusterAddrKey(), channel));
        ByteBuf hello = channel.alloc().ioBuffer(64);
        InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_CLUSTER, selfId);
        channel.writeAndFlush(hello).addListener((ChannelFutureListener) helloFuture -> {
          if (!helloFuture.isSuccess()) {
            channel.close();
            return;
          }
          channel.writeAndFlush(buildLockDeleteFrame(channel, name));
        });
      });
    }
  }

  @Override
  public void close() {
    for (Channel ch : channels.values()) {
      try {
        ch.close().syncUninterruptibly();
      } catch (Exception e) {
        LOG.warn("Не удалось закрыть канал: " + ch, e);
      }
    }
    channels.clear();
    group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
  }

  private static final class ClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private ClientHandler() {
      super(true);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
      if (!in.isReadable()) return;
      byte op = in.readByte();
      if (op == ClusterProtocol.OP_PONG) {
        ClusterPingPongCodec.readIdOrZero(in);
        return;
      }
      if (op == ClusterProtocol.OP_PING) {
        long id = ClusterPingPongCodec.readIdOrZero(in);
        ByteBuf out = ctx.alloc().ioBuffer(1 + 8);
        ClusterPingPongCodec.writePayload(out, ClusterProtocol.OP_PONG, id);
        ctx.writeAndFlush(out);
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent idleStateEvent) {
        if (idleStateEvent.state() == IdleState.WRITER_IDLE) {
          long id = System.nanoTime();
          ByteBuf out = ctx.alloc().ioBuffer(1 + 8);
          ClusterPingPongCodec.writePayload(out, ClusterProtocol.OP_PING, id);
          ctx.writeAndFlush(out);
          return;
        }
        if (idleStateEvent.state() == IdleState.READER_IDLE) {
          ctx.close();
          return;
        }
      }
      ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ctx.close();
    }
  }
}
