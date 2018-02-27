package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.nio.ByteBuffer;

class SerializedHTable {
  private final long                  htableSize;
  private final Pointer               htablePointer;
  private final ByteBuffer            htableBuffer;
  private final BloomFilter<byte[]>[] bloomFilters;

  SerializedHTable(long htableSize, Pointer htablePointer, ByteBuffer htableBuffer, BloomFilter<byte[]>[] bloomFilters) {
    this.htableSize = htableSize;
    this.htablePointer = htablePointer;
    this.htableBuffer = htableBuffer;
    this.bloomFilters = bloomFilters;
  }

  public ByteBuffer getHtableBuffer() {
    return htableBuffer;
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
