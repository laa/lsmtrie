package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class MemTable implements Table {
  @SuppressWarnings("NumericOverflow")
  private static final long FILL_BIT          = 1L << 63;
  private static final int  HEAP_DATA_OFFSET  = BUCKET_SIZE * BUCKETS_COUNT;
  private static final long ENTRY_COUNT_MASK  = 0x7FFFFL;
  private static final int  TABLE_SIZE_OFFSET = 19;

  private static final int   MAX_SIZE    = 64 * 1024 * 1024;
  private static final float LOAD_FACTOR = 0.9f;
  private static final int   MAX_ENTRIES = (int) (BUCKETS_COUNT * (BUCKET_SIZE / (ENTRY_SIZE)) * LOAD_FACTOR);

  private final AtomicLong state = new AtomicLong(((long) HEAP_DATA_OFFSET) << TABLE_SIZE_OFFSET);

  private final ConcurrentHashMap<KeyHolder, byte[]> map            = new ConcurrentHashMap<>();
  private final LongAdder                            modifiersCount = new LongAdder();
  private final long id;

  MemTable(long id) {
    this.id = id;
  }

  public boolean put(byte[] sha1, byte[] key, byte[] value) {
    if ((state.get() & FILL_BIT) != 0) {
      return false;
    }

    modifiersCount.increment();
    try {
      byte[] oldValue;

      while (true) {
        final long state = this.state.get();
        final boolean filled = (state & FILL_BIT) != 0;

        if (filled) {
          return false;
        }

        final long entries = state & ENTRY_COUNT_MASK;
        final long size = state >>> TABLE_SIZE_OFFSET;

        oldValue = map.get(new KeyHolder(key, sha1));

        long newSize = size;
        long newEntries = entries;

        if (oldValue != null) {
          newSize = newSize - oldValue.length + value.length;
        } else {
          newSize += value.length + key.length + 4;
          newEntries++;
        }

        if (newSize > MAX_SIZE || newEntries > MAX_ENTRIES) {
          final long newState = state | FILL_BIT;

          if (this.state.compareAndSet(state, newState)) {
            return false;
          }

          continue;
        }

        final long newState = newEntries | (newSize << TABLE_SIZE_OFFSET);

        if (this.state.compareAndSet(state, newState)) {
          break;
        }
      }

      final byte[] realOldValue = map.put(new KeyHolder(key, sha1), value);

      final int sizeDiff = (oldValue == null ? 0 : oldValue.length) - (realOldValue == null ? 0 : realOldValue.length);
      if (sizeDiff != 0) {
        while (true) {
          final long state = this.state.get();
          if ((state & FILL_BIT) != 0) {
            return true;//we have already added item so for current call there is no fill but no reason to track size anymore
          }

          final long entries = state & ENTRY_COUNT_MASK;
          final long size = state >>> TABLE_SIZE_OFFSET;

          assert entries <= MAX_ENTRIES;

          if (size + sizeDiff > MAX_SIZE) {
            final long newState = state | FILL_BIT;

            if (this.state.compareAndSet(state, newState)) {
              return true;
            }

            continue;
          }

          final long newState = entries | ((size + sizeDiff) << TABLE_SIZE_OFFSET);
          if (this.state.compareAndSet(state, newState)) {
            break;
          }
        }
      }

      return true;
    } finally {
      modifiersCount.decrement();
    }
  }

  public void remove(byte[] key, byte[] sha1) {
    throw new UnsupportedOperationException();
  }

  public void waitTillZeroModifiers() {
    while (modifiersCount.sum() > 0) {
      Thread.yield();
    }
  }

  @Override
  public byte[] get(byte[] key, byte[] sha1) {
    return map.get(new KeyHolder(key, null));
  }

  @Override
  public long getId() {
    return id;
  }

  int size() {
    return map.size();
  }

  boolean isEmpty() {
    return map.isEmpty();
  }

  int memorySize() {
    int size = HEAP_DATA_OFFSET;
    for (Map.Entry<KeyHolder, byte[]> entry : map.entrySet()) {
      size += 4 + entry.getKey().key.length + entry.getValue().length;
    }

    return size;
  }

  boolean isFilled() {
    return (state.get() & FILL_BIT) != 0;
  }

  public SerializedHTable toHTable() {
    int size = memorySize();

    final long ptr = Native.malloc(size);

    if (ptr == 0) {
      throw new IllegalStateException("Can not allocate memory for serialization of MemTable");
    }

    final Pointer pointer = new Pointer(ptr);

    Bucket[] buckets = new Bucket[BUCKETS_COUNT];
    @SuppressWarnings("unchecked")
    BloomFilter<byte[]>[] bloomFilters = new BloomFilter[BUCKETS_COUNT];

    boolean overloaded;

    for (int i = 0; i < BUCKETS_COUNT; i++) {
      buckets[i] = new Bucket(i);
      bloomFilters[i] = BloomFilter
          .create((Funnel<byte[]>) (bytes, primitiveSink) -> primitiveSink.putBytes(bytes), BUCKET_ENTRIES_COUNT, 0.001);
    }

    overloaded = fillHeapData(pointer, buckets, bloomFilters);

    if (overloaded) {
      moveOverloadedData(buckets);
    }

    fillInMapData(pointer, buckets);

    return new SerializedHTable(size, pointer, bloomFilters);
  }

  private void fillInMapData(Pointer pointer, Bucket[] buckets) {
    int counter = 0;
    for (Bucket bucket : buckets) {
      int entryOffset = counter * BUCKET_SIZE;

      pointer.setShort(entryOffset, (short) bucket.entries.size());
      entryOffset += 2;

      for (EntryRef entryRef : bucket.entries) {
        pointer.write(entryOffset, entryRef.sha1, 0, entryRef.sha1.length);
        entryOffset += entryRef.sha1.length;
        pointer.setInt(entryOffset, entryRef.entryOffset);
        entryOffset += 4;
      }

      entryOffset = (counter + 1) * BUCKET_SIZE - DEST_BUCKET_OFFSET;

      pointer.setInt(entryOffset, bucket.destBucket);
      entryOffset += 4;

      pointer.setLong(entryOffset, bucket.waterMark);
      counter++;
    }
  }

  private void moveOverloadedData(Bucket[] buckets) {
    Bucket[] sortedBuckets = new Bucket[BUCKETS_COUNT];

    System.arraycopy(buckets, 0, sortedBuckets, 0, sortedBuckets.length);
    Arrays.sort(sortedBuckets, Comparator.comparingInt(o -> o.entries.size()));

    int minBucketIndex = 0;
    sortedBuckets[0].entries.sort(Comparator.comparingLong(EntryRef::getWaterMark));

    for (int i = sortedBuckets.length - 1; i >= 0; i--) {
      Bucket bucket = sortedBuckets[i];

      if (bucket.entries.size() > BUCKET_ENTRIES_COUNT) {
        bucket.entries.sort(Comparator.comparingLong(EntryRef::getWaterMark));
        minBucketIndex = moveOverloadedEntries(sortedBuckets, minBucketIndex, bucket);
      } else {
        break;
      }
    }
  }

  private boolean fillHeapData(Pointer pointer, Bucket[] buckets, BloomFilter<byte[]>[] bloomFilters) {
    int dataOffset = HEAP_DATA_OFFSET;

    boolean overloaded = false;
    for (Map.Entry<KeyHolder, byte[]> entry : map.entrySet()) {
      byte[] sha1 = entry.getKey().sha1;
      final int bucketIndex = HashUtils.bucketIndex(sha1);

      Bucket bucket = buckets[bucketIndex];

      final EntryRef entryRef = new EntryRef();
      entryRef.sha1 = sha1;
      entryRef.entryOffset = dataOffset;

      bucket.entries.add(entryRef);

      final byte[] key = entry.getKey().key;
      final byte[] value = entry.getValue();

      pointer.setShort(dataOffset, (short) key.length);
      dataOffset += 2;
      pointer.write(dataOffset, key, 0, key.length);
      dataOffset += key.length;

      pointer.setShort(dataOffset, (short) value.length);
      dataOffset += 2;
      pointer.write(dataOffset, value, 0, value.length);
      dataOffset += value.length;

      final BloomFilter<byte[]> bloomFilter = bloomFilters[bucketIndex];
      bloomFilter.put(key);

      overloaded = overloaded || bucket.entries.size() > BUCKET_ENTRIES_COUNT;
    }

    return overloaded;
  }

  private int moveOverloadedEntries(Bucket[] buckets, int minBucketIndex, Bucket bucket) {
    final List<EntryRef> entryRefs = bucket.entries;
    final ListIterator<EntryRef> iterator = entryRefs.listIterator(entryRefs.size());

    EntryRef entryRef = null;

    final Bucket minBucket = buckets[minBucketIndex];
    final List<EntryRef> minEntries = minBucket.entries;

    if (minEntries.size() >= BUCKET_ENTRIES_COUNT) {
      throw new IllegalStateException("Bucket which is supposed to be underloaded is actually overloaded");
    }

    while (entryRefs.size() > BUCKET_ENTRIES_COUNT) {
      if (entryRef == null) {
        entryRef = iterator.previous();
      }

      final long waterMark = entryRef.getWaterMark();

      int indexToInsert = Collections.binarySearch(minEntries, entryRef, Comparator.comparingLong(EntryRef::getWaterMark));

      if (indexToInsert < 0) {
        indexToInsert = -indexToInsert - 1;
      }

      while (entryRef.getWaterMark() == waterMark) {
        iterator.remove();
        minEntries.add(indexToInsert, entryRef);

        if (iterator.hasPrevious()) {
          entryRef = iterator.previous();
        }
      }

      bucket.waterMark = waterMark;
      bucket.destBucket = minBucket.index;
    }

    if (minEntries.size() >= BUCKET_ENTRIES_COUNT) {
      minBucketIndex++;

      if (minBucketIndex < buckets.length) {
        buckets[minBucketIndex].entries.sort(Comparator.comparingLong(EntryRef::getWaterMark));
      } else {
        throw new IllegalStateException("All buckets are overloaded");
      }
    }

    if (minEntries.size() > BUCKET_ENTRIES_COUNT) {
      minBucketIndex = moveOverloadedEntries(buckets, minBucketIndex, minBucket);
    }

    return minBucketIndex;
  }

  private class Bucket {
    final int index;

    long waterMark;
    int destBucket = -1;

    List<EntryRef> entries = new ArrayList<>();

    Bucket(int index) {
      this.index = index;
    }
  }

  private class EntryRef {
    private long waterMark = -1;
    private byte[] sha1;
    private int    entryOffset;

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
}


