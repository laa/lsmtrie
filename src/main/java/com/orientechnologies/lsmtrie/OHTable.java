package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class OHTable implements OTable {
  private final BloomFilter<byte[]>[] bloomFilters;
  private final ByteBuffer            buffer;
  private final long                  id;

  OHTable(BloomFilter<byte[]>[] bloomFilters, ByteBuffer buffer, long id) {
    this.bloomFilters = bloomFilters;
    this.buffer = buffer;
    this.id = id;
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    final int bucketIndexMask = 0x3;
    final int bucketIndex = ((sha1[1] & bucketIndexMask) << 8) | (0xFF & sha1[0]);
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
    final byte[] data = new byte[BUCKET_SIZE];

    final ByteBuffer htable = buffer.duplicate().order(ByteOrder.nativeOrder());
    htable.position(index * BUCKET_SIZE);
    htable.get(data);

    final ByteBuffer bucket = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
    final int destBucket = getDestBucket(bucket);
    if (destBucket >= 0) {
      final long waterMark = getWaterMark(bucket);
      final long entryWaterMark = OHashUtils.generateWaterMarkHash(sha1);

      if (entryWaterMark >= waterMark) {
        return readValueFormBucket(key, sha1, destBucket);
      }
    }

    bucket.position(0);

    final int entriesCount = bucket.getShort();

    //TODO: read directly form ByteBuffer array do not copy and compare
    for (int i = 0; i < entriesCount; i++) {
      final byte[] shaPosition = new byte[ENTRY_SIZE];
      bucket.get(shaPosition);

      boolean equals = true;
      for (int n = 0; n < SHA_1_SIZE; n++) {
        if (shaPosition[n] != sha1[n]) {
          equals = false;
          break;
        }
      }

      if (equals) {
        bucket.position(bucket.position() - DATA_OFFSET_SIZE);

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
    //TODO: hash water mark data in concurrent hash map to avoiding of reading of whole bucket
    final int offset = BUCKET_SIZE - WATER_MARK_OFFSET;
    bucket.position(offset);

    return bucket.getLong();
  }
}
