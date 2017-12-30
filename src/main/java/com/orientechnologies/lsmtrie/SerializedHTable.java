package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.nio.ByteBuffer;

class SerializedHTable {
  private final long                  htableSize;
  private final Pointer               htablePointer;
  private final BloomFilter<byte[]>[] bloomFilters;

  SerializedHTable(long htableSize, Pointer htablePointer, BloomFilter<byte[]>[] bloomFilters) {
    this.htableSize = htableSize;
    this.htablePointer = htablePointer;
    this.bloomFilters = bloomFilters;
  }

  public ByteBuffer getHtableBuffer() {
    return htablePointer.getByteBuffer(0, htableSize);
  }

  public BloomFilter<byte[]>[] getBloomFilters() {
    return bloomFilters;
  }

  public long getHtableSize() {
    return htableSize;
  }

  public void free() {
    Native.free(Pointer.nativeValue(htablePointer));
  }
}
