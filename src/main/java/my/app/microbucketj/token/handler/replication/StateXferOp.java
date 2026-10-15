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

public class StateXferOp implements OpcodeHandler {

  private final BucketRegistry buckets;

  public StateXferOp(BucketRegistry buckets) {
    this.buckets = Objects.requireNonNull(buckets, "buckets");
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_STATE_XFER;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenReplicationCodec.readEpoch(in);
    BucketState st = ReplPutOp.readState(in);
    AbstractBucketRuntime rt = buckets.get(st.bucket());
    if (rt == null) return;
    rt.applyReplica(st);
  }
}
