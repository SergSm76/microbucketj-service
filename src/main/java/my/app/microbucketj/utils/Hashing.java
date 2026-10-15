package my.app.microbucketj.utils;

import io.netty.buffer.ByteBuf;

public final class Hashing {

  private Hashing() {
  }

  // FNV-1a 64 over bytes
  public static long fnv1a64(byte[] data, int off, int len) {
    long h = 0xcbf29ce484222325L;
    for (int i = 0; i < len; i++) {
      h ^= (data[off + i] & 0xff);
      h *= 0x100000001b3L;
    }
    return mix64(h);
  }

  public static long fnv1a64(ByteBuf in, int idx, int len) {
    long h = 0xcbf29ce484222325L;
    for (int i = 0; i < len; i++) {
      h ^= (in.getByte(idx + i) & 0xff);
      h *= 0x100000001b3L;
    }
    return Hashing.mix64(h);
  }

  // splitmix64 finalizer
  public static long mix64(long z) {
    z += 0x9e3779b97f4a7c15L;
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }

  // rendezvous score
  public static long score(long keyHash, long nodeHash) {
    return mix64(keyHash ^ nodeHash);
  }
}
