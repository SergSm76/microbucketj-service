package my.app.microbucketj.token.handler.replication;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketRegistry;
import my.app.microbucketj.bucket.AbstractBucketRuntime;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.dispatch.OpcodeHandler;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.token.TokenReplicationCodec;

import java.util.Objects;

public final class ReplPutOp implements OpcodeHandler {

  private final BucketRegistry buckets;

  public ReplPutOp(BucketRegistry buckets) {
    this.buckets = Objects.requireNonNull(buckets, "buckets");
  }

  static BucketState readState(ByteBuf in) {
    return TokenReplicationCodec.readState(in);
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_REPL_PUT;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    int corrId = TokenReplicationCodec.readCorrId(in);
    TokenReplicationCodec.readEpoch(in);

    BucketState st = readState(in);
    AbstractBucketRuntime rt = buckets.get(st.bucket());
    if (rt == null) {
      TokenReplicationCodec.writeReplAck(ctx, corrId, false);
      return;
    }

    boolean ok = true;
    try {
      rt.applyReplica(st);
    } catch (Exception e) {
      ok = false;
    }
    TokenReplicationCodec.writeReplAck(ctx, corrId, ok);
  }
}
