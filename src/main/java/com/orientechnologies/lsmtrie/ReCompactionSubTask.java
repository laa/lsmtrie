package com.orientechnologies.lsmtrie;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class ReCompactionSubTask extends RecursiveAction {
  private MemTable memTable;

  private final List<byte[][]>[] buckets;
  private final int[]            endIntervals;
  private       int[]            processedItems;
  private       int[]            startIntervals;
  private boolean complete = false;
  private int     inserted = 0;

  ReCompactionSubTask(MemTable memTable, List<byte[][]>[] buckets, int[] startIntervals, int[] endIntervals) {
    this.memTable = memTable;
    this.buckets = buckets;
    this.endIntervals = endIntervals;

    this.processedItems = new int[startIntervals.length];
    this.startIntervals = new int[startIntervals.length];

    System.arraycopy(startIntervals, 0, processedItems, 0, processedItems.length);
  }

  @Override
  protected void compute() {
    System.arraycopy(processedItems, 0, this.startIntervals, 0, startIntervals.length);

    inserted = 0;
    try {
      boolean atLeastOneIntervalProcessed = true;
      while (atLeastOneIntervalProcessed) {
        atLeastOneIntervalProcessed = false;

        for (int i = 0; i < buckets.length; i++) {
          final List<byte[][]> bucket = buckets[i];

          final int nextItem = processedItems[i];

          if (nextItem < endIntervals[i]) {
            final byte[][] entry = bucket.get(nextItem);

            if (!memTable.put(entry[0], entry[1], entry[2])) {
              return;
            }

            assert Arrays.equals(memTable.get(entry[1], entry[0]), entry[2]);

            inserted++;
            atLeastOneIntervalProcessed = true;
            processedItems[i]++;
          }
        }
      }

      complete = true;
    } catch (Exception | Error e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void setMemTable(MemTable memTable) {
    this.memTable = memTable;
  }

  public boolean isComplete() {
    return complete;
  }
}
