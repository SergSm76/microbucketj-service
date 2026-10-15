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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplPutBatchOp implements OpcodeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ReplPutBatchOp.class);
  private final BucketRegistry buckets;

  public ReplPutBatchOp(BucketRegistry buckets) {
    this.buckets = Objects.requireNonNull(buckets, "buckets");
  }

  @Override
  public byte opcode() {
    return TokenProtocol.OP_REPL_PUT_BATCH;
  }

  @Override
  public void handle(ChannelHandlerContext ctx, ByteBuf in) {
    TokenReplicationCodec.readEpoch(in);
    BucketState st = ReplPutOp.readState(in);
    AbstractBucketRuntime rt = buckets.get(st.bucket());
    if (rt == null) return;
    try {
      rt.applyReplica(st);
    } catch (Exception e) {
      LOG.warn("Не удалось применить состояние реплики для бакета: " + st.bucket(), e);
    }
  }
}
