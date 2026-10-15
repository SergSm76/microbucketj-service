package my.app.microbucketj.token;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.cluster.model.Node;
import my.app.microbucketj.protocol.InternalProtocol;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.internal.InternalHelloCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardAcquireNameCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardReportHashCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardReportNameCodec;
import my.app.microbucketj.protocol.codec.token.TokenForwardResponseCodec;
import my.app.microbucketj.protocol.codec.token.TokenPingPongCodec;
import my.app.microbucketj.protocol.codec.token.TokenReplicationCodec;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class TokenPeerClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TokenPeerClient.class);

  private static final String PRIMARY_ERR_MSG = "primary cannot be null";
  private static final String REPLICA_ERR_MSG = "replica cannot be null";
  private static final String STATE_ERR_MSG = "state cannot be null";
  private static final String TARGET_ERR_MSG = "target cannot be null";

  private static final ForwardResult UNAVAILABLE =
      new ForwardResult(TokenProtocol.ST_UNAVAILABLE, 0, 10, true);
  private final String selfId;
  private final long forwardTimeoutMs;
  private final EventLoopGroup group;
  private final Bootstrap bootstrap;
  private final long connectBackoffNanos;
  private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> connectInFlight = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> reconnectAllowedAtNanos = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, PendingForward> pendingForward = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, PendingAck> pendingAck = new ConcurrentHashMap<>();
  private final AtomicInteger corrSeq = new AtomicInteger(1);
  public TokenPeerClient(int ioThreads,
                         int maxFrameSize,
                         long heartbeatIntervalMs,
                         long heartbeatTimeoutMs,
                         String selfId,
                         long connectBackoffMs,
                         long forwardTimeoutMs) {
    boolean epoll = Epoll.isAvailable();
    IoHandlerFactory ioFactory = epoll ? EpollIoHandler.newFactory() : NioIoHandler.newFactory();
    this.selfId = (selfId == null) ? "" : selfId;
    this.forwardTimeoutMs = Math.max(1, forwardTimeoutMs);
    this.connectBackoffNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(100, connectBackoffMs));

    this.group = new MultiThreadIoEventLoopGroup(Math.max(1, ioThreads / 2),
        new DefaultThreadFactory("mbj-tpc-io", 10),
        ioFactory);

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
            p.addLast(new ClientHandler(pendingForward, pendingAck));
          }
        });
  }

  private static ByteBuf buildStateXfer(Channel ch, int epoch, BucketState st) {
    ByteBuf out = ch.alloc().ioBuffer(256);
    TokenReplicationCodec.writeStateXfer(out, epoch, st);
    return out;
  }

  public CompletableFuture<ForwardResult> forwardAcquire(Node primary, long permits, String bucket) {
    return forwardAcquire(primary, permits, bucket, forwardTimeoutMs);
  }

  public CompletableFuture<ForwardResult> forwardAcquireByHash(Node primary, long permits, long bucketHash) {
    return forwardAcquireByHash(primary, permits, bucketHash, forwardTimeoutMs);
  }

  public CompletableFuture<ForwardResult> forwardReport(Node primary, boolean success, String bucket) {
    return forwardReport(primary, success, bucket, forwardTimeoutMs);
  }

  public CompletableFuture<ForwardResult> forwardReportByHash(Node primary, boolean success, long bucketHash) {
    return forwardReportByHash(primary, success, bucketHash, forwardTimeoutMs);
  }

  public void connectAll(Iterable<Node> nodes, Node self) {
    for (Node n : nodes) {
      if (n.id().equals(self.id())) continue;
      connect(n);
    }
  }

  public void connect(Node n) {
    String k = n.clusterAddrKey();
    Channel existing = channels.get(k);
    if (existing != null && existing.isActive()) return;
    Long reconnectAt = reconnectAllowedAtNanos.get(k);
    long now = System.nanoTime();
    if (reconnectAt != null && now < reconnectAt) return;
    if (connectInFlight.putIfAbsent(k, Boolean.TRUE) != null) return;

    bootstrap.connect(n.host(), n.clusterPort()).addListener((ChannelFutureListener) f -> {
      connectInFlight.remove(k);
      if (!f.isSuccess()) {
        reconnectAllowedAtNanos.put(k, System.nanoTime() + connectBackoffNanos);
        LOG.debug("Не удалось выполнить token connect к {}:{}", n.host(), n.clusterPort(), f.cause());
        return;
      }

      Channel ch = f.channel();
      ByteBuf hello = ch.alloc().ioBuffer(64);
      InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_TOKEN, selfId);
      ch.writeAndFlush(hello);

      channels.put(k, ch);
      reconnectAllowedAtNanos.remove(k);
      ch.closeFuture().addListener((ChannelFutureListener) cf -> {
        channels.remove(k, ch);
        reconnectAllowedAtNanos.put(k, System.nanoTime() + connectBackoffNanos);
      });
    });
  }

  public boolean isConnected(Node n) {
    if (n == null) return false;
    Channel ch = channels.get(n.clusterAddrKey());
    return ch != null && ch.isActive();
  }

  public CompletableFuture<ForwardResult> forwardAcquire(Node primary, long permits, String bucket, long timeoutMs) {
    Objects.requireNonNull(primary, PRIMARY_ERR_MSG);
    if (bucket == null) bucket = "";

    Channel ch = channels.get(primary.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(primary);
      return CompletableFuture.completedFuture(UNAVAILABLE);
    }

    int corrId = corrSeq.getAndIncrement();
    CompletableFuture<ForwardResult> fut = new CompletableFuture<>();
    pendingForward.put(corrId, new PendingForward(ch, fut));

    ByteBuf out = ch.alloc().ioBuffer(64);
    TokenForwardAcquireNameCodec.writeRequest(out, corrId, permits, bucket);
    writeForward(ch, corrId, fut, out, timeoutMs);
    return fut;
  }

  public CompletableFuture<ForwardResult> forwardAcquireByHash(Node primary, long permits, long bucketHash, long timeoutMs) {
    Objects.requireNonNull(primary, PRIMARY_ERR_MSG);

    Channel ch = channels.get(primary.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(primary);
      return CompletableFuture.completedFuture(UNAVAILABLE);
    }

    int corrId = corrSeq.getAndIncrement();
    CompletableFuture<ForwardResult> fut = new CompletableFuture<>();
    pendingForward.put(corrId, new PendingForward(ch, fut));

    ByteBuf out = ch.alloc().ioBuffer(1 + 4 + 8 + 8);
    TokenForwardAcquireHashCodec.writeRequest(out, corrId, permits, bucketHash);
    writeForward(ch, corrId, fut, out, timeoutMs);
    return fut;
  }

  public CompletableFuture<ForwardResult> forwardReport(Node primary, boolean success, String bucket, long timeoutMs) {
    Objects.requireNonNull(primary, PRIMARY_ERR_MSG);
    if (bucket == null) bucket = "";

    Channel ch = channels.get(primary.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(primary);
      return CompletableFuture.completedFuture(UNAVAILABLE);
    }

    int corrId = corrSeq.getAndIncrement();
    CompletableFuture<ForwardResult> fut = new CompletableFuture<>();
    pendingForward.put(corrId, new PendingForward(ch, fut));

    ByteBuf out = ch.alloc().ioBuffer(64);
    TokenForwardReportNameCodec.writeRequest(out, corrId, success, bucket);
    writeForward(ch, corrId, fut, out, timeoutMs);
    return fut;
  }

  public CompletableFuture<ForwardResult> forwardReportByHash(Node primary, boolean success, long bucketHash, long timeoutMs) {
    Objects.requireNonNull(primary, PRIMARY_ERR_MSG);

    Channel ch = channels.get(primary.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(primary);
      return CompletableFuture.completedFuture(UNAVAILABLE);
    }

    int corrId = corrSeq.getAndIncrement();
    CompletableFuture<ForwardResult> fut = new CompletableFuture<>();
    pendingForward.put(corrId, new PendingForward(ch, fut));

    ByteBuf out = ch.alloc().ioBuffer(1 + 4 + 1 + 8);
    TokenForwardReportHashCodec.writeRequest(out, corrId, success, bucketHash);
    writeForward(ch, corrId, fut, out, timeoutMs);
    return fut;
  }

  private void writeForward(Channel ch, int corrId, CompletableFuture<ForwardResult> fut, ByteBuf out, long timeoutMs) {
    ch.writeAndFlush(out).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) return;
      PendingForward removed = pendingForward.remove(corrId);
      if (removed != null) removed.fut.complete(UNAVAILABLE);
    });
    ch.eventLoop().schedule(() -> {
      PendingForward removed = pendingForward.remove(corrId);
      if (removed != null) removed.fut.complete(UNAVAILABLE);
    }, Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
  }

  public CompletableFuture<Ack> replicatePut(Node replica, int epoch, BucketState st, long timeoutMs) {
    Objects.requireNonNull(replica, REPLICA_ERR_MSG);
    Objects.requireNonNull(st, STATE_ERR_MSG);

    Channel ch = channels.get(replica.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(replica);
      return CompletableFuture.completedFuture(new Ack(false));
    }

    int corrId = corrSeq.getAndIncrement();
    CompletableFuture<Ack> fut = new CompletableFuture<>();
    pendingAck.put(corrId, new PendingAck(ch, fut));

    ByteBuf out = ch.alloc().ioBuffer(256);
    TokenReplicationCodec.writeReplPut(out, corrId, epoch, st);

    ch.writeAndFlush(out).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) return;
      PendingAck removed = pendingAck.remove(corrId);
      if (removed != null) removed.fut.complete(new Ack(false));
    });

    ch.eventLoop().schedule(() -> {
      PendingAck removed = pendingAck.remove(corrId);
      if (removed != null) removed.fut.complete(new Ack(false));
    }, Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);

    return fut;
  }

  public void replicatePutBatch(Node replica, int epoch, BucketState st) {
    Objects.requireNonNull(replica, REPLICA_ERR_MSG);
    Objects.requireNonNull(st, STATE_ERR_MSG);

    Channel ch = channels.get(replica.clusterAddrKey());
    if (ch == null || !ch.isActive()) {
      connect(replica);
      return;
    }

    ByteBuf out = ch.alloc().ioBuffer(256);
    TokenReplicationCodec.writeReplPutBatch(out, epoch, st);
    ch.writeAndFlush(out);
  }

  public void replicatePutBestEffort(Node replica, int epoch, BucketState st) {
    replicatePutBatch(replica, epoch, st);
  }

  public void stateTransfer(Node target, int epoch, BucketState st) {
    Objects.requireNonNull(target, TARGET_ERR_MSG);
    Objects.requireNonNull(st, STATE_ERR_MSG);

    String key = target.clusterAddrKey();
    Channel ch = channels.get(key);

    if (ch == null || !ch.isActive()) {
      bootstrap.connect(target.host(), target.clusterPort()).addListener((ChannelFutureListener) f -> {
        if (!f.isSuccess()) {
          LOG.debug("Не удалось выполнить token stateTransfer connect к {}:{}", target.host(), target.clusterPort(), f.cause());
          return;
        }
        Channel c = f.channel();
        channels.put(key, c);
        c.closeFuture().addListener((ChannelFutureListener) cf -> channels.remove(key, c));

        ByteBuf hello = c.alloc().ioBuffer(64);
        InternalHelloCodec.writePayload(hello, InternalProtocol.ROLE_TOKEN, selfId);

        c.writeAndFlush(hello).addListener((ChannelFutureListener) hf -> {
          if (!hf.isSuccess()) {
            c.close();
            return;
          }
          c.writeAndFlush(buildStateXfer(c, epoch, st));
        });
      });
      return;
    }

    ch.writeAndFlush(buildStateXfer(ch, epoch, st));
  }

  @Override
  public void close() {
    for (Map.Entry<String, Channel> e : channels.entrySet()) {
      try {
        e.getValue().close();
      } catch (Exception exception) {
        LOG.error("Ошибка закрытия token channel {}", e.getKey(), exception);
      }
    }
    channels.clear();

    pendingForward.forEach((id, p) -> p.fut.complete(UNAVAILABLE));
    pendingForward.clear();

    pendingAck.forEach((id, p) -> p.fut.complete(new Ack(false)));
    pendingAck.clear();

    group.shutdownGracefully();
  }

  public record ForwardResult(byte status, long remaining, long retryAfterMs, boolean unavailable) {
  }

  public record Ack(boolean ok) {
  }

  private record PendingForward(Channel ch, CompletableFuture<ForwardResult> fut) {
  }

  private record PendingAck(Channel ch, CompletableFuture<Ack> fut) {
  }

  private static final class ClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final ConcurrentHashMap<Integer, PendingForward> pendingForward;
    private final ConcurrentHashMap<Integer, PendingAck> pendingAck;

    ClientHandler(ConcurrentHashMap<Integer, PendingForward> pendingForward,
                  ConcurrentHashMap<Integer, PendingAck> pendingAck) {
      super(true);
      this.pendingForward = pendingForward;
      this.pendingAck = pendingAck;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
      if (!in.isReadable()) return;
      byte op = in.readByte();

      if (op == TokenProtocol.OP_PONG) {
        TokenPingPongCodec.readIdOrZero(in);
        return;
      }

      if (op == TokenProtocol.OP_PING) {
        long id = TokenPingPongCodec.readIdOrZero(in);
        ByteBuf out = ctx.alloc().ioBuffer(1 + 8);
        TokenPingPongCodec.writePayload(out, TokenProtocol.OP_PONG, id);
        ctx.writeAndFlush(out);
        return;
      }

      if (op == TokenProtocol.OP_FWD_RESP) {
        TokenForwardResponseCodec.ForwardResponse resp = TokenForwardResponseCodec.read(in);
        PendingForward p = pendingForward.remove(resp.requestId());
        if (p != null) {
          p.fut.complete(new ForwardResult(resp.status(), resp.remaining(), resp.retryAfterMs(), resp.status() == TokenProtocol.ST_UNAVAILABLE));
        }
        return;
      }

      if (op == TokenProtocol.OP_REPL_ACK) {
        int corrId = TokenReplicationCodec.readCorrId(in);
        boolean ok = TokenReplicationCodec.readAckOk(in);
        PendingAck p = pendingAck.remove(corrId);
        if (p != null) p.fut.complete(new Ack(ok));
      }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent e) {
        if (e.state() == IdleState.WRITER_IDLE) {
          long id = System.nanoTime();
          ByteBuf out = ctx.alloc().ioBuffer(1 + 8);
          TokenPingPongCodec.writePayload(out, TokenProtocol.OP_PING, id);
          ctx.writeAndFlush(out);
          return;
        }
        if (e.state() == IdleState.READER_IDLE) {
          ctx.close();
          return;
        }
      }
      ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      Channel ch = ctx.channel();

      pendingForward.forEach((id, p) -> {
        if (p.ch == ch && pendingForward.remove(id, p)) {
          p.fut.complete(UNAVAILABLE);
        }
      });

      pendingAck.forEach((id, p) -> {
        if (p.ch == ch && pendingAck.remove(id, p)) {
          p.fut.complete(new Ack(false));
        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ctx.close();
    }
  }
}
