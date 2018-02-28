package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

class SerializedHTable {
  private final long                  htableSize;
  private final ByteBuffer            htableBuffer;
  private final BloomFilter<byte[]>[] bloomFilters;

  SerializedHTable(long htableSize, ByteBuffer htableBuffer, BloomFilter<byte[]>[] bloomFilters) {
    this.htableSize = htableSize;
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
    final DirectBuffer dbf = (DirectBuffer) htableBuffer;
    final Cleaner cleaner = dbf.cleaner();
    cleaner.clean();
  }
}
