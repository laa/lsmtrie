package com.orientechnologies.lsmtrie;

import java.util.concurrent.RecursiveAction;

public class CompactionDataMoveSubTask extends RecursiveAction {
  private final MemTable[] memTables;
  private final HTable     hTable;
  private final int        bucketIndex;
  private final int        startEntryIndex;
  private final int        level;

  private int     nextProcessedItem;
  private boolean complete;

  CompactionDataMoveSubTask(MemTable[] memTables, HTable hTable, int bucketIndex, int startEntryIndex, int level) {
    this.memTables = memTables;
    this.hTable = hTable;
    this.bucketIndex = bucketIndex;
    this.startEntryIndex = startEntryIndex;
    this.level = level;
  }

  @Override
  protected void compute() {
    nextProcessedItem = startEntryIndex;
    int bucketLength = hTable.bucketLength(bucketIndex);

    while (nextProcessedItem < bucketLength) {
      final byte[][] entry = hTable.getBucketItem(bucketIndex, nextProcessedItem);
      final int memIndex = HashUtils.childNodeIndex(level, entry[0]);
      final MemTable memTable = memTables[memIndex];
      final boolean added = memTable.put(entry[0], entry[1], entry[2]);

      if (!added) {
        return;
      }

      nextProcessedItem++;
    }

    complete = true;
  }

  public int getBucketIndex() {
    return bucketIndex;
  }

  public int getNextProcessedItem() {
    return nextProcessedItem;
  }

  public boolean isComplete() {
    return complete;
  }
}
