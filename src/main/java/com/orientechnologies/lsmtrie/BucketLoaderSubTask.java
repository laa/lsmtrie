package com.orientechnologies.lsmtrie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveAction;

public class BucketLoaderSubTask extends RecursiveAction {
  private static final Set<EntryHolder>[] buckets;

  static {
    @SuppressWarnings("unchecked")
    final Set<EntryHolder>[] bs = new Set[1024];

    for (int i = 0; i < bs.length; i++) {
      bs[i] = Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    buckets = bs;
  }

  private final List<HTable> hTables;
  private final int          bucketIndex;

  private final List<byte[][]> bucket         = new ArrayList<>();
  private       boolean        collectionMode = true;

  BucketLoaderSubTask(List<HTable> hTables, int bucketIndex) {
    this.hTables = hTables;
    this.bucketIndex = bucketIndex;
  }

  @Override
  protected void compute() {
    if (collectionMode) {
      for (HTable hTable : hTables) {
        final int len = hTable.bucketLength(bucketIndex);

        for (int n = 0; n < len; n++) {
          final byte[][] entry = hTable.getBucketItem(bucketIndex, n);
          final int itemBucketIndex = HashUtils.bucketIndex(entry[0]);

          final Set<EntryHolder> bucket = buckets[itemBucketIndex];
          bucket.add(new EntryHolder(entry));
        }
      }

      collectionMode = false;
    } else {
      for (EntryHolder entryHolder : buckets[bucketIndex]) {
        bucket.add(entryHolder.entry);
      }
    }
  }

  public List<byte[][]> getBucket() {
    return bucket;
  }

  private static class EntryHolder {
    private final byte[][] entry;

    private int hash = 0;

    EntryHolder(byte[][] entry) {
      this.entry = entry;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      EntryHolder that = (EntryHolder) o;
      return Arrays.equals(entry[1], that.entry[1]);
    }

    @Override
    public int hashCode() {
      if (hash == 0) {
        hash = Arrays.hashCode(entry[0]);
      }

      return hash;
    }
  }
}
