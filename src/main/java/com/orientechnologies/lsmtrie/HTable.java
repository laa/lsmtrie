package com.orientechnologies.lsmtrie;

import com.concurrencyfreaks.locks.ScalableRWLock;
import com.google.common.hash.BloomFilter;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class HTable implements Table {
  private final BloomFilter<byte[]>[] bloomFilters;
  private final ByteBuffer            buffer;
  private final long                  id;
  private final ConcurrentHashMap<Short, WaterMarkInfo> overloadingMap = new ConcurrentHashMap<>();

  private final Path bloomFilterPath;
  private final Path htablePath;

  private final ScalableRWLock modificationLock = new ScalableRWLock();

  HTable(BloomFilter<byte[]>[] bloomFilters, ByteBuffer buffer, long id, Path bloomFilterPath, Path htablePath) {
    this.bloomFilters = bloomFilters;
    this.buffer = buffer;
    this.id = id;
    this.bloomFilterPath = bloomFilterPath;
    this.htablePath = htablePath;
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    final boolean locked = modificationLock.sharedTryLock();
    if (!locked) {
      return null;
    }

    try {
      final int bucketIndex = HashUtils.bucketIndex(sha1);
      final BloomFilter<byte[]> bloomFilter = bloomFilters[bucketIndex];

      if (bloomFilter.mightContain(sha1)) {
        return readValueFormBucket(key, sha1, bucketIndex);
      }

      return null;
    } finally {
      modificationLock.sharedUnlock();
    }
  }

  public int bucketLength(int index) {
    modificationLock.sharedLock();
    try {
      final ByteBuffer htable = buffer.duplicate().order(ByteOrder.nativeOrder());
      htable.position(index * BUCKET_SIZE);

      return 0xFFFF & htable.getShort();
    } finally {
      modificationLock.sharedUnlock();
    }
  }

  public Iterator<byte[][]> bucketIterator(final int bucketIndex) {
    final Iterator<byte[][]> iterator;

    modificationLock.sharedLock();
    try {
      iterator = new Iterator<byte[][]>() {
        final int entriesCount;

        final byte[] data;
        final ByteBuffer bucket;
        final ByteBuffer htable;

        int currentPosition;
        int processedEntries;

        {
          htable = buffer.duplicate().order(ByteOrder.nativeOrder());
          htable.position(bucketIndex * BUCKET_SIZE);

          data = new byte[BUCKET_SIZE];
          htable.get(data);

          bucket = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
          entriesCount = 0xFFFF & bucket.getShort();
          currentPosition = BUCKET_ENTRIES_COUNT_SIZE;
        }

        @Override
        public boolean hasNext() {
          return processedEntries < entriesCount;
        }

        @Override
        public byte[][] next() {
          if (processedEntries >= entriesCount) {
            throw new NoSuchElementException();
          }

          final byte entryType = data[currentPosition];
          currentPosition++;

          if (entryType == EMBEDDED_ENTREE_TYPE) {
            final int keyLength = data[currentPosition] & 0xFF;
            currentPosition++;

            final byte[] key = new byte[keyLength];
            System.arraycopy(data, currentPosition, key, 0, keyLength);
            currentPosition += keyLength;

            final int valueLength = data[currentPosition] & 0xFF;
            currentPosition++;

            final byte[] value = new byte[valueLength];
            System.arraycopy(data, currentPosition, value, 0, valueLength);
            currentPosition += valueLength;

            processedEntries++;

            return new byte[][] { null, key, value };
          }

          if (entryType == HEAP_ENTREE_TYPE) {
            final byte[] sha1 = new byte[SHA_1_SIZE];
            System.arraycopy(data, 0, sha1, 0, SHA_1_SIZE);
            currentPosition += SHA_1_SIZE;

            final int heapReference = bucket.getInt(currentPosition);
            currentPosition += HEAP_REFERENCE_LENGTH;

            final byte[] key;
            final byte[] value;

            modificationLock.sharedLock();
            try {
              htable.position(heapReference);

              final int keyLength = htable.getShort() & 0xFFFF;
              key = new byte[keyLength];
              htable.get(key);

              final int valueLength = htable.getShort() & 0xFFFF;
              value = new byte[valueLength];
              htable.get(value);
            } finally {
              modificationLock.sharedUnlock();
            }

            processedEntries++;
            return new byte[][] { sha1, key, value };
          }

          throw new IllegalStateException("Illegal entry type " + entryType);
        }
      };

    } finally {
      modificationLock.sharedUnlock();
    }

    return iterator;
  }

  @Override
  public long getId() {
    return id;
  }

  public void clearBuffer() {
    modificationLock.exclusiveLock();
    final DirectBuffer dbf = (DirectBuffer) buffer;
    final Cleaner cleaner = dbf.cleaner();
    cleaner.clean();
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
    final int entriesCount = bucket.getShort() & 0xFFFF;
    int position = 2;

    for (int i = 0; i < entriesCount; i++) {
      final byte entryType = data[position];
      position++;

      if (entryType == EMBEDDED_ENTREE_TYPE) {
        final int keyLength = data[position] & 0xFF;
        position++;

        boolean found = true;

        if (keyLength == key.length) {
          for (int n = 0; n < keyLength; n++) {
            if (key[n] != data[n + position]) {
              found = false;
              break;
            }
          }
        } else {
          found = false;
        }

        position += keyLength;
        final int valueLength = data[position] & 0xFF;
        position++;

        if (found) {
          final byte[] value = new byte[valueLength];
          System.arraycopy(data, position, value, 0, valueLength);

          return value;
        } else {
          position += valueLength;
        }
      } else if (entryType == HEAP_ENTREE_TYPE) {
        final int shaStart = position;
        boolean found = true;
        for (int n = 0; n < SHA_1_SIZE; n++) {
          if (sha1[n] != data[shaStart + n]) {
            found = false;
            break;
          }
        }

        position += SHA_1_SIZE;
        if (found) {
          bucket.position(position);
          final int heapRef = bucket.getInt();
          position += HEAP_REFERENCE_LENGTH;

          htable.position(heapRef);

          final int keyLength = htable.getShort() & 0xFFFF;
          final byte[] entryKey = new byte[keyLength];
          htable.get(entryKey);

          if (Arrays.equals(key, entryKey)) {
            final int valueLength = htable.getShort() & 0xFFFF;

            final byte[] value = new byte[valueLength];
            htable.get(value);

            return value;
          }
        } else {
          position += HEAP_REFERENCE_LENGTH;
        }
      } else {
        throw new IllegalStateException("Invalid entry type " + entryType);
      }
    }

    return null;
  }

  private int getDestBucket(ByteBuffer bucket) {
    final int offset = BUCKET_SIZE - DEST_BUCKET_OFFSET;
    bucket.position(offset);

    return bucket.getShort();
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
