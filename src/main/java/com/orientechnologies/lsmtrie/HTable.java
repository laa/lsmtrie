package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class HTable implements Table {
  private final BloomFilter<byte[]>[] bloomFilters;
  private final ByteBuffer            buffer;
  private final long                  id;
  private final ConcurrentHashMap<Short, WaterMarkInfo> overloadingMap = new ConcurrentHashMap<>();

  HTable(BloomFilter<byte[]>[] bloomFilters, ByteBuffer buffer, long id) {
    this.bloomFilters = bloomFilters;
    this.buffer = buffer;
    this.id = id;
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    final int bucketIndex = HashUtils.getBucketIndex(sha1);
    final BloomFilter<byte[]> bloomFilter = bloomFilters[bucketIndex];

    if (bloomFilter.mightContain(key)) {
      return readValueFormBucket(key, sha1, bucketIndex);
    }

    return null;
  }

  @Override
  public long getId() {
    return id;
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
