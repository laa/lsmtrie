package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class HTable implements Table {
  private final BloomFilter<byte[]>[] bloomFilters;
  private final ByteBuffer            buffer;
  private final long                  id;
  private final ConcurrentHashMap<Short, WaterMarkInfo> overloadingMap = new ConcurrentHashMap<>();

  private final Path bloomFilterPath;
  private final Path htablePath;

  private final LongAdder readersCount = new LongAdder();

  HTable(BloomFilter<byte[]>[] bloomFilters, ByteBuffer buffer, long id, Path bloomFilterPath, Path htablePath) {
    this.bloomFilters = bloomFilters;
    this.buffer = buffer;
    this.id = id;
    this.bloomFilterPath = bloomFilterPath;
    this.htablePath = htablePath;
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    readersCount.increment();
    try {
      final int bucketIndex = HashUtils.bucketIndex(sha1);
      final BloomFilter<byte[]> bloomFilter = bloomFilters[bucketIndex];

      if (bloomFilter.mightContain(key)) {
        return readValueFormBucket(key, sha1, bucketIndex);
      }

      return null;
    } finally {
      readersCount.decrement();
    }
  }

  public int bucketLength(int index) {
    readersCount.increment();
    try {
      final ByteBuffer htable = buffer.duplicate().order(ByteOrder.nativeOrder());
      htable.position(index * BUCKET_SIZE);

      return htable.getShort();
    } finally {
      readersCount.decrement();
    }
  }

  public byte[][] getBucketItem(int bucketIndex, int entryIndex) {
    readersCount.increment();
    try {
      final ByteBuffer htable = buffer.duplicate().order(ByteOrder.nativeOrder());
      htable.position(bucketIndex * BUCKET_SIZE + ENTRY_SIZE * entryIndex + 2);

      final byte[] sha1 = new byte[SHA_1_SIZE];
      htable.get(sha1);

      final int entryOffset = htable.getInt();

      htable.position(entryOffset);
      final int keyLength = htable.getShort();
      final byte[] key = new byte[keyLength];
      htable.get(key);

      final int valueLength = htable.getShort();
      final byte[] value = new byte[valueLength];
      htable.get(value);

      final byte[][] result = new byte[3][];
      result[0] = sha1;
      result[1] = key;
      result[2] = value;

      return result;
    } finally {
      readersCount.decrement();
    }
  }

  public void waitTillReaders() {
    while (readersCount.sum() > 0) {
      Thread.yield();
    }
  }

  @Override
  public long getId() {
    return id;
  }

  public void clearBuffer() {
    final DirectBuffer dbf = (DirectBuffer) buffer;
    final Cleaner cleaner = dbf.cleaner();
    if (cleaner != null) {
      cleaner.clean();
    }
  }

  public Path getBloomFilterPath() {
    return bloomFilterPath;
  }

  public Path getHtablePath() {
    return htablePath;
  }

  private byte[] readValueFormBucket(byte[] key, byte[] sha1, int index) {
    final WaterMarkInfo waterMarkInfo = overloadingMap.get((short) index);
    if (waterMarkInfo != null) {
      final long waterMark = waterMarkInfo.waterMark;
      final long entryWaterMark = HashUtils.generateWaterMarkHash(sha1);
      if (entryWaterMark >= waterMark) {
        return readValueFormBucket(key, sha1, waterMarkInfo.destId);
      }
    }

    final byte[] data = new byte[BUCKET_SIZE];

    final ByteBuffer htable = buffer.duplicate().order(ByteOrder.nativeOrder());
    htable.position(index * BUCKET_SIZE);
    htable.get(data);

    final ByteBuffer bucket = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    if (waterMarkInfo == null) {
      final int destBucket = getDestBucket(bucket);

      if (destBucket >= 0) {
        final long waterMark = getWaterMark(bucket);
        overloadingMap.put((short) index, new WaterMarkInfo((short) destBucket, waterMark));

        final long entryWaterMark = HashUtils.generateWaterMarkHash(sha1);
        if (entryWaterMark >= waterMark) {
          return readValueFormBucket(key, sha1, destBucket);
        }
      }
    }

    bucket.position(0);
    final int entriesCount = bucket.getShort();

    for (int i = 0; i < entriesCount; i++) {
      int offset = 2 + ENTRY_SIZE * i;
      boolean equals = true;
      for (int n = 0; n < SHA_1_SIZE; n++) {
        if (data[offset] != sha1[n]) {
          equals = false;
          break;
        }

        offset++;
      }

      offset += DATA_OFFSET_SIZE;

      if (equals) {
        bucket.position(offset - DATA_OFFSET_SIZE);

        final int entryOffset = bucket.getInt();

        htable.position(entryOffset);
        final int keyLength = htable.getShort();
        final byte[] entryKey = new byte[keyLength];
        htable.get(entryKey);

        if (Arrays.equals(key, entryKey)) {
          final int valueLength = htable.getShort();
          final byte[] value = new byte[valueLength];
          htable.get(value);

          return value;
        }
      }
    }

    return null;
  }

  private int getDestBucket(ByteBuffer bucket) {
    final int offset = BUCKET_SIZE - DEST_BUCKET_OFFSET;
    bucket.position(offset);

    return bucket.getInt();
  }

  private long getWaterMark(ByteBuffer bucket) {
    final int offset = BUCKET_SIZE - WATER_MARK_OFFSET;
    bucket.position(offset);

    return bucket.getLong();
  }

  private final class WaterMarkInfo {
    private final short destId;
    private final long  waterMark;

    WaterMarkInfo(short destId, long waterMark) {
      this.destId = destId;
      this.waterMark = waterMark;
    }
  }
}
