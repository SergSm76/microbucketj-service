package my.app.microbucketj.protocol.codec.token;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import my.app.microbucketj.bucket.BucketState;
import my.app.microbucketj.bucket.BucketStateWire;
import my.app.microbucketj.protocol.TokenProtocol;
import my.app.microbucketj.protocol.codec.common.FramedResponseWriter;

public final class TokenReplicationCodec {

  private TokenReplicationCodec() {
  }

  public static int readCorrId(ByteBuf in) {
    return in.readInt();
  }

  public static int readEpoch(ByteBuf in) {
    return in.readInt();
  }

  public static BucketState readState(ByteBuf in) {
    return BucketStateWire.read(in);
  }

  public static void writeReplPut(ByteBuf out, int corrId, int epoch, BucketState state) {
    out.writeByte(TokenProtocol.OP_REPL_PUT);
    out.writeInt(corrId);
    out.writeInt(epoch);
    BucketStateWire.write(out, state);
  }

  public static void writeReplPutBatch(ByteBuf out, int epoch, BucketState state) {
    out.writeByte(TokenProtocol.OP_REPL_PUT_BATCH);
    out.writeInt(epoch);
    BucketStateWire.write(out, state);
  }

  public static void writeStateXfer(ByteBuf out, int epoch, BucketState state) {
    out.writeByte(TokenProtocol.OP_STATE_XFER);
    out.writeInt(epoch);
    BucketStateWire.write(out, state);
  }

  public static void writeReplAck(ChannelHandlerContext ctx, int corrId, boolean ok) {
    ByteBuf out = FramedResponseWriter.allocate(ctx, TokenProtocol.REPL_ACK_PAYLOAD_LEN);
    out.writeByte(TokenProtocol.OP_REPL_ACK);
    out.writeInt(corrId);
    out.writeByte(ok ? 1 : 0);
    ctx.writeAndFlush(out);
  }

  public static boolean readAckOk(ByteBuf in) {
    return in.readByte() == 1;
  }
}
