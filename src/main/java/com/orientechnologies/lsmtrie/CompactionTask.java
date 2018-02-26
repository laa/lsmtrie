package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionTask extends RecursiveAction {
  private final String               name;
  private final Semaphore            compactionCounter;
  private final AtomicBoolean        stop;
  private final PriorityQueue<NodeN> compactionQueue;
  private final Node0                node0;
  private final AtomicLong           tableIdGen;
  private final Path                 root;

  CompactionTask(String name, Semaphore compactionCounter, AtomicBoolean stop, Node0 node0, AtomicLong tableIdGen, Path root) {
    this.name = name;
    this.compactionCounter = compactionCounter;
    this.stop = stop;
    this.node0 = node0;
    this.tableIdGen = tableIdGen;
    this.root = root;
    this.compactionQueue = new PriorityQueue<>(Comparator.comparingInt(NodeN::getLevel));
  }

  @Override
  protected void compute() {
    try {
      while (!stop.get()) {
        try {
          //wait till 8 htables in node 0 before start node 0 compaction
          final boolean compactionLimitIsReached = compactionCounter.tryAcquire(8, 1, TimeUnit.SECONDS);

          if (compactionLimitIsReached) {
            moveHTablesDown(node0);

            NodeN node;

            while ((node = compactionQueue.poll()) != null) {
              moveHTablesDown(node);
            }
          }
        } catch (IOException e) {
          throw new IllegalStateException("Error during compaction", e);
        } catch (InterruptedException e) {
          return;
        }
      }
    } catch (Exception | Error e) {
      e.printStackTrace();
      throw e;
    }
  }

  private void moveHTablesDown(Node node) throws IOException {
    final long start = System.nanoTime();
    int newTables = 0;

    final MemTable[] memTables = new MemTable[8];
    for (int i = 0; i < memTables.length; i++) {
      memTables[i] = new MemTable(tableIdGen.getAndIncrement());
    }

    final List<HTable> hTables = node.getNOldestHTables(8);
    System.out.printf("Compaction is started for node from level %d which contains %d htables\n", node.getLevel(), hTables.size());

    if (hTables.size() < 8) {
      System.out.println("Compaction is finished nothing to compact");
      return;
    }

    if (node instanceof Node0) {
      try {
        compactionCounter.acquire(hTables.size() - 8);
      } catch (InterruptedException e) {
        return;
      }
    } else {
      ((NodeN) node).setLastDuplicationCheck(0);
    }

    final int[] initialTablesCount = new int[8];
    final NodeN[] children = node.getChildren();

    for (int i = 0; i < 8; i++) {
      initialTablesCount[i] = children[i].hTablesCount();
    }

    for (HTable hTable : hTables) {
      boolean isMoved = false;

      int[][] bucketsStartIndex = new int[1024][2];

      for (int i = 0; i < 1024; i++) {
        bucketsStartIndex[i] = new int[] { i, 0 };
      }

      while (!isMoved) {
        final List<CompactionDataMoveSubTask> moveSubTasks = new ArrayList<>();
        for (int[] entry : bucketsStartIndex) {
          moveSubTasks.add(new CompactionDataMoveSubTask(memTables, hTable, entry[0], entry[1], node.getLevel() + 1));
        }

        ForkJoinTask.invokeAll(moveSubTasks);

        for (int i = 0; i < children.length; i++) {
          final NodeN child = children[i];

          final MemTable memTable = memTables[i];
          if (memTable.isFilled()) {
            final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name, node.getLevel() + 1);
            convert.invoke();

            final HTable newTable = convert.gethTable();
            newTables++;
            child.addHTable(newTable);

            memTables[i] = new MemTable(tableIdGen.getAndIncrement());
          }
        }

        final List<int[]> notCompletedBuckets = new ArrayList<>();
        for (CompactionDataMoveSubTask subTask : moveSubTasks) {
          if (!subTask.isComplete()) {
            notCompletedBuckets.add(new int[] { subTask.getBucketIndex(), subTask.getNextProcessedItem() });
          }
        }

        isMoved = notCompletedBuckets.isEmpty();
        if (!isMoved) {
          bucketsStartIndex = notCompletedBuckets.toArray(new int[][] {});
        }
      }
    }

    for (int i = 0; i < children.length; i++) {
      final NodeN child = children[i];
      final MemTable memTable = memTables[i];

      if (!memTable.isEmpty()) {
        final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name, node.getLevel() + 1);
        convert.invoke();

        final HTable newTable = convert.gethTable();
        newTables++;
        child.addHTable(newTable);
      }
    }

    for (HTable hTableToRemove : hTables) {
      node.removeTable(hTableToRemove.getId());
    }

    long duplicateCheckSum = 0;
    long reCompactionTime = 0;

    for (int i = 0; i < 8; i++) {
      final NodeN child = children[i];
      int hTablesCount = child.hTablesCount();

      if (child.getLevel() > 1) {
        if (initialTablesCount[i] == 0) {
          child.setLastDuplicationCheck(hTablesCount);
        }

        if (hTablesCount > 1 && initialTablesCount[i] > 0 && hTablesCount >= 2 * child.getLastDuplicationCheck()) {
          final long startDuplicateCheck = System.nanoTime();

          child.setLastDuplicationCheck(hTablesCount);

          //check for duplicates
          final List<HTable> hTableList = child.getNOldestHTables(hTablesCount);

          final List<DuplicatesCheckerSubTask> duplicatesCheckers = new ArrayList<>();
          for (int n = 0; n < 1024; n++) {
            duplicatesCheckers.add(new DuplicatesCheckerSubTask(hTableList, n));
          }

          ForkJoinTask.invokeAll(duplicatesCheckers);

          int uniqueItems = 0;
          int totalItems = 0;

          for (DuplicatesCheckerSubTask checker : duplicatesCheckers) {
            uniqueItems += checker.getUniqueItems();
            totalItems += checker.getAllItems();
          }

          final long endDuplicateCheck = System.nanoTime();
          duplicateCheckSum += (endDuplicateCheck - startDuplicateCheck);

          if (totalItems >= 2 * uniqueItems) {
            System.out.printf("Amount of duplications is exceeded for the node level: %d index %d. "
                + "Total items %d, unique items %d. Re-compaction is started\n", child.getLevel(), i, totalItems, uniqueItems);
            final long reCompactStart = System.nanoTime();
            newTables += reCompactItems(child, uniqueItems);
            final long reCompactionEnd = System.nanoTime();
            reCompactionTime += reCompactionEnd - reCompactStart;
          }
        }
      }

      hTablesCount = child.hTablesCount();

      if (hTablesCount >= 8) {
        compactionQueue.add(child);
      }
    }
    final long end = System.nanoTime();
    System.out.printf(
        "Compaction is finished in %d ms. ,%d tables were compacted, duplicates check time %d ms, recompaction time %d ms., %d tables were created \n",
        (end - start) / 1000_000, hTables.size(), duplicateCheckSum / 1000_000, reCompactionTime / 1000_000, newTables);
  }

  private int reCompactItems(NodeN node, int uniqueItems) throws IOException {
    int newTables = 0;

    final List<HTable> hTables = node.getHTables();

    final List<BucketLoaderSubTask> bucketLoaders = new ArrayList<>();

    for (int i = 0; i < 1024; i++) {
      bucketLoaders.add(new BucketLoaderSubTask(hTables, i));
    }

    ForkJoinTask.invokeAll(bucketLoaders);

    for (BucketLoaderSubTask subTask : bucketLoaders) {
      subTask.reinitialize();
    }

    ForkJoinTask.invokeAll(bucketLoaders);

    @SuppressWarnings("unchecked")
    final List<byte[][]>[] buckets = new List[1024];

    //compact htables into buckets
    for (int i = 0; i < buckets.length; i++) {
      final BucketLoaderSubTask loaderSubTask = bucketLoaders.get(i);
      buckets[i] = loaderSubTask.getBucket();
    }

    List<ReCompactionSubTask> reCompactionSubTasks = new ArrayList<>();
    MemTable memTable = new MemTable(tableIdGen.getAndIncrement());

    boolean atLeastOneIntervalPresent = true;
    int[] startIntervals = new int[1024];

    final int intervalStep = 10;
    while (atLeastOneIntervalPresent) {
      atLeastOneIntervalPresent = false;

      int[] endIntervals = new int[1024];
      for (int i = 0; i < 1024; i++) {
        final int startIndex = startIntervals[i];
        final List<byte[][]> bucket = buckets[i];

        final int bucketSize = bucket.size();

        if (startIndex >= bucketSize) {
          endIntervals[i] = bucketSize;
          continue;
        }

        final int endIndex = startIndex + intervalStep;

        if (endIndex < bucketSize) {
          endIntervals[i] = endIndex;
        } else {
          endIntervals[i] = bucketSize;
        }

        assert endIntervals[i] <= bucketSize;
        assert endIntervals[i] - startIntervals[i] >= 0;
        assert startIntervals[i] >= 0;

        if (!atLeastOneIntervalPresent) {
          atLeastOneIntervalPresent = startIndex < bucketSize;
        }
      }

      if (atLeastOneIntervalPresent) {
        reCompactionSubTasks.add(new ReCompactionSubTask(memTable, buckets, startIntervals, endIntervals));

        System.arraycopy(endIntervals, 0, startIntervals, 0, startIntervals.length);
      }
    }

    ForkJoinTask.invokeAll(reCompactionSubTasks);

    while (true) {
      if (memTable.isFilled()) {
        final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name, node.getLevel());
        convert.invoke();

        final HTable newTable = convert.gethTable();
        newTables++;
        node.addHTable(newTable);

        memTable = new MemTable(tableIdGen.getAndIncrement());
      }

      reCompactionSubTasks.removeIf(ReCompactionSubTask::isComplete);

      if (reCompactionSubTasks.isEmpty()) {
        break;
      }

      for (ReCompactionSubTask reCompactionSubTask : reCompactionSubTasks) {
        reCompactionSubTask.setMemTable(memTable);
        reCompactionSubTask.reinitialize();
      }

      ForkJoinTask.invokeAll(reCompactionSubTasks);
    }

    if (!memTable.isEmpty()) {
      final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name, node.getLevel());
      convert.invoke();

      final HTable newTable = convert.gethTable();
      newTables++;
      node.addHTable(newTable);
    }

    for (HTable hTable : hTables) {
      node.removeTable(hTable.getId());
    }

    return newTables;
  }
}
