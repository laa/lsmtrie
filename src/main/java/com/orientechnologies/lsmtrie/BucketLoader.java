package com.orientechnologies.lsmtrie;

import java.util.concurrent.RecursiveAction;

public class BucketLoader extends RecursiveAction {
  private final int      bucketIndex;
  private final HTable   hTable;
  private final MemTable memTable;

  public BucketLoader(int bucketIndex, HTable hTable, MemTable memTable) {
    this.bucketIndex = bucketIndex;
    this.hTable = hTable;
    this.memTable = memTable;
  }

  @Override
  protected void compute() {
    try {
      final int bucketLen = hTable.bucketLength(bucketIndex);

      for (int i = 0; i < bucketLen; i++) {
        final byte[][] entry = hTable.getBucketItem(bucketIndex, i);
        final boolean added = memTable.put(entry[0], entry[1], entry[2]);
        assert added;
      }
    } catch (Exception | Error e) {
      e.printStackTrace();
      throw e;
    }
  }
}
