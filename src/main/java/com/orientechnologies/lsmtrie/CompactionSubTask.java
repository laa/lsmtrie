package com.orientechnologies.lsmtrie;

import java.security.MessageDigest;
import java.util.Iterator;
import java.util.concurrent.RecursiveAction;

public class CompactionSubTask extends RecursiveAction {
  private final MemTable[]         memTables;
  private final HTable             hTable;
  private final int                bucketIndex;
  private       Iterator<byte[][]> bucketIterator;
  private final int                level;
  private       boolean            complete;
  private byte[][] nextToAdd = null;

  CompactionSubTask(MemTable[] memTables, HTable hTable, int bucketIndex, int level) {
    this.memTables = memTables;
    this.hTable = hTable;
    this.bucketIndex = bucketIndex;
    this.level = level;
  }

  @Override
  protected void compute() {
    try {
      if (bucketIterator == null) {
        bucketIterator = hTable.bucketIterator(bucketIndex);
      }

      if (nextToAdd != null) {
        final int memIndex = HashUtils.childNodeIndex(level, nextToAdd[0]);
        final MemTable memTable = memTables[memIndex];
        final boolean added = memTable.put(nextToAdd[0], nextToAdd[1], nextToAdd[2]);

        if (!added) {
          return;
        }

        nextToAdd = null;
      }

      while (bucketIterator.hasNext()) {
        nextToAdd = bucketIterator.next();

        if (nextToAdd[0] == null) {
          MessageDigest digest = MessageDigestHolder.instance().get();
          digest.reset();

          nextToAdd[0] = digest.digest(nextToAdd[1]);
        }

        final int memIndex = HashUtils.childNodeIndex(level, nextToAdd[0]);
        final MemTable memTable = memTables[memIndex];
        final boolean added = memTable.put(nextToAdd[0], nextToAdd[1], nextToAdd[2]);

        if (!added) {
          return;
        }

        nextToAdd = null;
      }

      complete = true;
    } catch (Exception | Error e) {
      e.printStackTrace();
      throw e;
    }
  }

  public boolean isComplete() {
    return complete;
  }
}
