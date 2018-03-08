package com.orientechnologies.lsmtrie;

import com.concurrencyfreaks.locks.ScalableRWLock;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.util.concurrent.Striped;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class MemTable implements Table {
  @SuppressWarnings("NumericOverflow")
  private static final long FILL_BIT = 1L << 63;

  private static final int SEGMENT_SIZE_MASK = 0xFF_FF_FF;//24 bit

  private static final int HEAP_DATA_OFFSET = BUCKET_SIZE * BUCKETS_COUNT;

  private static final int SEGMENT_SIZE_OFFSET = 24;

  private static final int   MAX_SIZE    = 64 * 1024 * 1024;
  private static final float LOAD_FACTOR = 0.9f;

  private static final int MAX_ENTRY_SIZE = 256;

  private static final int SEGMENT_SIZE_LIMIT = (int) (SEGMENT_SIZE * LOAD_FACTOR);

  private final AtomicLong state = new AtomicLong(0);

  private static final int SUPPOSED_ENTRY_COUNT = (BUCKET_SIZE / 100) * BUCKETS_COUNT;

  private final ConcurrentHashMap<KeyHolder, byte[]> map              = new ConcurrentHashMap<>(SUPPOSED_ENTRY_COUNT);
  private final ScalableRWLock                       modificationLock = new ScalableRWLock();
  private final long id;
  private final Striped<Lock> entryLock = Striped.lazyWeakLock(SUPPOSED_ENTRY_COUNT);

  MemTable(long id) {
    this.id = id;
  }

  public boolean put(byte[] sha1, byte[] key, byte[] value) {
    if ((state.get() & FILL_BIT) != 0) {
      return false;
    }

    final boolean locked = modificationLock.sharedTryLock();
    if (!locked) {
      return false;
    }
    try {
      final Lock lock = entryLock.get(sha1);
      lock.lock();
      try {
        final byte[] oldValue = map.get(new KeyHolder(key, sha1));

        while (true) {
          final long state = this.state.get();
          final boolean filled = (state & FILL_BIT) != 0;

          if (filled) {
            return false;
          }

          final int segmentSize = (int) (state & SEGMENT_SIZE_MASK);
          final int heapSize = (int) (state >>> SEGMENT_SIZE_OFFSET);

          final int[] sizes = calculateNewSize(segmentSize, heapSize, key, value, oldValue);
          final int newSegmentSize = sizes[0];
          final int newHeapSize = sizes[1];

          if (newSegmentSize > SEGMENT_SIZE_LIMIT || newSegmentSize + newHeapSize > MAX_SIZE) {
            final long newState = state | FILL_BIT;

            if (this.state.compareAndSet(state, newState)) {
              return false;
            }

            continue;
          }

          final long newState = (newHeapSize << SEGMENT_SIZE_OFFSET) | newSegmentSize;

          if (this.state.compareAndSet(state, newState)) {
            map.put(new KeyHolder(key, sha1), value);

            return true;
          }
        }
      } finally {
        lock.unlock();
      }
    } finally {
      modificationLock.sharedUnlock();
    }
  }

  private int[] calculateNewSize(int segmentSize, int heapSize, byte[] key, byte[] value, byte[] oldValue) {
    final int keyEntreeLength = key.length + value.length;

    int newSegmentSize = segmentSize;
    int newHeapSize = heapSize;

    if (keyEntreeLength <= MAX_ENTRY_SIZE) {
      //embedded case
      if (oldValue == null) {
        newSegmentSize += ENTRY_TYPE_LENGTH + KEY_LENGTH + key.length + VALUE_LENGTH + value.length;
      } else {
        if (oldValue.length + key.length <= MAX_ENTRY_SIZE) {
          newSegmentSize += -oldValue.length + value.length;
        } else {
          newSegmentSize += -HEAP_REFERENCE_LENGTH - SHA_1_SIZE + KEY_LENGTH + key.length + VALUE_LENGTH + value.length;
          newHeapSize -= HEAP_KEY_LENGTH + key.length + HEAP_VALUE_LENGTH + oldValue.length;
        }
      }
    } else {
      if (oldValue == null) {
        newSegmentSize += ENTRY_TYPE_LENGTH + SHA_1_SIZE + HEAP_REFERENCE_LENGTH;
        newHeapSize += HEAP_KEY_LENGTH + key.length + HEAP_VALUE_LENGTH + value.length;
      } else {
        if (oldValue.length + key.length <= MAX_ENTRY_SIZE) {
          newSegmentSize += -KEY_LENGTH - key.length - VALUE_LENGTH - oldValue.length + SHA_1_SIZE + HEAP_REFERENCE_LENGTH;
          newHeapSize += HEAP_KEY_LENGTH + key.length + HEAP_VALUE_LENGTH + value.length;
        } else {
          newHeapSize += -oldValue.length + value.length;
        }
      }
    }

    return new int[] { newSegmentSize, newHeapSize };
  }

  public void remove(byte[] key, byte[] sha1) {
    throw new UnsupportedOperationException();
  }

  public void waitTillZeroModifiers() {
    modificationLock.exclusiveLock();
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    return map.get(new KeyHolder(key, null));
  }

  @Override
  public long getId() {
    return id;
  }

  boolean isNotEmpty() {
    return !map.isEmpty();
  }

  private int memorySize() {
    int size = HEAP_DATA_OFFSET;

    for (Map.Entry<KeyHolder, byte[]> entry : map.entrySet()) {
      final int entryLength = entry.getKey().key.length + entry.getValue().length;

      if (entryLength > MAX_ENTRY_SIZE) {
        size += HEAP_KEY_LENGTH + HEAP_VALUE_LENGTH;
      }
    }

    return size;
  }

  boolean isFilled() {
    return (state.get() & FILL_BIT) != 0;
  }

  public SerializedHTable toHTable() {
    final int size = memorySize();
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());

    Bucket[] buckets = new Bucket[BUCKETS_COUNT];
    @SuppressWarnings("unchecked")

    boolean overloaded;

    for (int i = 0; i < BUCKETS_COUNT; i++) {
      buckets[i] = new Bucket(i);
    }

    overloaded = fillHeapDataAndSplitEntriesBetweenBuckets(buffer, buckets);

    @SuppressWarnings("unchecked")
    final BloomFilter<byte[]>[] bloomFilters = new BloomFilter[BUCKETS_COUNT];
    for (int i = 0; i < bloomFilters.length; i++) {
      final Bucket bucket = buckets[i];

      final BloomFilter<byte[]> filter = BloomFilter.create(new BytesFunnel(), bucket.entries.size(), 0.001);
      bloomFilters[i] = filter;

      for (BucketEntry entry : bucket.entries) {
        filter.put(entry.sha1);
      }
    }

    if (overloaded) {
      moveOverloadedData(buckets);
    }

    fillInSegmentData(buffer, buckets);

    return new SerializedHTable(size, buffer, bloomFilters);
  }

  private void fillInSegmentData(ByteBuffer buffer, Bucket[] buckets) {
    for (int i = 0; i < buckets.length; i++) {
      final Bucket bucket = buckets[i];

      buffer.position(i * BUCKET_SIZE);
      buffer.putShort((short) bucket.entries.size());

      for (BucketEntry entry : bucket.entries) {
        buffer.put(entry.entryType);

        if (entry.entryType == EMBEDDED_ENTREE_TYPE) {
          buffer.put((byte) entry.key.length);
          buffer.put(entry.key);
          buffer.put((byte) entry.value.length);
          buffer.put(entry.value);
        } else {
          buffer.put(entry.sha1);
          buffer.putInt(entry.entryOffset);
        }
      }

      buffer.position((i + 1) * BUCKET_SIZE - DEST_BUCKET_OFFSET);
      buffer.putShort((short) bucket.destBucket);

      buffer.putLong(bucket.waterMark);
    }
  }

  private void moveOverloadedData(Bucket[] buckets) {
    Bucket[] sortedBuckets = new Bucket[BUCKETS_COUNT];

    System.arraycopy(buckets, 0, sortedBuckets, 0, sortedBuckets.length);
    Arrays.sort(sortedBuckets, Comparator.comparingInt(o -> o.segmentSize));

    int minBucketIndex = 0;
    sortedBuckets[0].entries.sort(Comparator.comparingLong(BucketEntry::getWaterMark));

    for (int i = sortedBuckets.length - 1; i >= 0; i--) {
      Bucket bucket = sortedBuckets[i];

      if (bucket.segmentSize > BUCKET_DATA_SIZE_LIMIT) {
        bucket.entries.sort(Comparator.comparingLong(BucketEntry::getWaterMark));
        minBucketIndex = moveOverloadedEntries(sortedBuckets, minBucketIndex, bucket);
      } else {
        break;
      }
    }
  }

  private boolean fillHeapDataAndSplitEntriesBetweenBuckets(ByteBuffer buffer, Bucket[] buckets) {
    buffer.position(HEAP_DATA_OFFSET);

    boolean overloaded = false;
    for (Map.Entry<KeyHolder, byte[]> entry : map.entrySet()) {
      final KeyHolder keyHolder = entry.getKey();

      final byte[] key = keyHolder.key;
      final byte[] sha1 = keyHolder.sha1;

      final byte[] value = entry.getValue();

      final int bucketIndex = HashUtils.bucketIndex(sha1);

      Bucket bucket = buckets[bucketIndex];

      final BucketEntry bucketEntry = new BucketEntry();
      bucketEntry.sha1 = sha1;
      bucketEntry.key = key;
      bucketEntry.value = value;

      if (key.length + value.length <= MAX_ENTRY_SIZE) {
        bucketEntry.entryOffset = -1;
        bucketEntry.entryLength = ENTRY_TYPE_LENGTH + KEY_LENGTH + key.length + VALUE_LENGTH + value.length;
        bucketEntry.entryType = EMBEDDED_ENTREE_TYPE;
      } else {
        bucketEntry.entryOffset = buffer.position();

        buffer.putShort((short) key.length);
        buffer.put(key);

        buffer.putShort((short) value.length);
        buffer.put(value);

        bucketEntry.entryLength = ENTRY_TYPE_LENGTH + SHA_1_SIZE + HEAP_REFERENCE_LENGTH;
        bucketEntry.entryType = HEAP_ENTREE_TYPE;
      }

      bucket.entries.add(bucketEntry);
      bucket.segmentSize += bucketEntry.entryLength;

      overloaded = overloaded || bucket.segmentSize > BUCKET_DATA_SIZE_LIMIT;
    }

    return overloaded;
  }

  private int moveOverloadedEntries(Bucket[] buckets, int minBucketIndex, Bucket bucket) {
    final List<BucketEntry> entries = bucket.entries;
    final ListIterator<BucketEntry> iterator = entries.listIterator(entries.size());

    BucketEntry entry = null;

    final Bucket minBucket = buckets[minBucketIndex];
    final List<BucketEntry> minEntries = minBucket.entries;

    if (minBucket.segmentSize >= BUCKET_DATA_SIZE_LIMIT) {
      throw new IllegalStateException("Bucket which is supposed to be underloaded is actually overloaded");
    }

    while (bucket.segmentSize > BUCKET_DATA_SIZE_LIMIT) {
      if (entry == null) {
        entry = iterator.previous();
      }

      final long waterMark = entry.getWaterMark();

      int indexToInsert = Collections.binarySearch(minEntries, entry, Comparator.comparingLong(BucketEntry::getWaterMark));

      if (indexToInsert < 0) {
        indexToInsert = -indexToInsert - 1;
      }

      while (entry.getWaterMark() == waterMark) {
        iterator.remove();
        minEntries.add(indexToInsert, entry);

        bucket.segmentSize -= entry.entryLength;
        minBucket.segmentSize += entry.entryLength;

        if (iterator.hasPrevious()) {
          entry = iterator.previous();
        }
      }

      bucket.waterMark = waterMark;
      bucket.destBucket = minBucket.index;
    }

    if (minBucket.segmentSize >= BUCKET_DATA_SIZE_LIMIT) {
      minBucketIndex++;

      if (minBucketIndex < buckets.length) {
        buckets[minBucketIndex].entries.sort(Comparator.comparingLong(BucketEntry::getWaterMark));
      } else {
        throw new IllegalStateException("All buckets are overloaded");
      }
    }

    if (minBucket.segmentSize > BUCKET_DATA_SIZE_LIMIT) {
      minBucketIndex = moveOverloadedEntries(buckets, minBucketIndex, minBucket);
    }

    return minBucketIndex;
  }

  private class Bucket {
    final int index;

    long waterMark;
    int destBucket = -1;
    int segmentSize;

    List<BucketEntry> entries = new ArrayList<>();

    Bucket(int index) {
      this.index = index;
    }
  }

  private class BucketEntry {
    private long waterMark = -1;
    private byte[] sha1;
    private byte[] key;
    private byte[] value;

    private int  entryOffset;
    private int  entryLength;
    private byte entryType;

    private long getWaterMark() {
      if (waterMark == -1) {
        waterMark = HashUtils.generateWaterMarkHash(sha1);
      }

      return waterMark;
    }
  }

  class KeyHolder {
    private final byte[] key;
    private final byte[] sha1;

    KeyHolder(byte[] key, byte[] sha1) {
      this.key = key;
      this.sha1 = sha1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      KeyHolder keyHolder = (KeyHolder) o;
      return Arrays.equals(key, keyHolder.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }
  }

  private static class BytesFunnel implements Funnel<byte[]> {

    @Override
    @ParametersAreNonnullByDefault
    public void funnel(byte[] from, PrimitiveSink into) {
      into.putBytes(from);
    }
  }
}


