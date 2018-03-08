package com.orientechnologies.lsmtrie;

import java.security.MessageDigest;
import java.util.Iterator;
import java.util.concurrent.RecursiveAction;

public class BucketLoader extends RecursiveAction {
  private final int      bucketIndex;
  private final HTable   hTable;
  private final MemTable memTable;

  BucketLoader(int bucketIndex, HTable hTable, MemTable memTable) {
    this.bucketIndex = bucketIndex;
    this.hTable = hTable;
    this.memTable = memTable;
  }

  @Override
  protected void compute() {
    try {
      final Iterator<byte[][]> bucketIterator = hTable.bucketIterator(bucketIndex);

      while (bucketIterator.hasNext()) {

        final byte[][] entry = bucketIterator.next();
        if (entry[0] == null) {
          MessageDigest digest = MessageDigestHolder.instance().get();
          digest.reset();

          entry[0] = digest.digest(entry[1]);
        }

        final boolean added = memTable.put(entry[0], entry[1], entry[2]);
        assert added;
      }
    } catch (Exception | Error e) {
      e.printStackTrace();
      throw e;
    }
  }
}
