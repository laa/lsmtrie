package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionTask extends RecursiveAction {
  private final String               name;
  private final Semaphore            compactionCounter;
  private final AtomicBoolean        stop;
  private final PriorityQueue<NodeN> compactionQueue;
  private final Node0                node0;
  private final AtomicLong           tableIdGen;
  private final Path                 root;
  private final Set<NodeN> compactionSet = new HashSet<>();

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
          final boolean compactFirstLevel = compactionCounter.tryAcquire(8, 1, TimeUnit.SECONDS);

          if (compactFirstLevel) {
            moveHTablesDown(node0);
          } else {
            final NodeN node = compactionQueue.poll();
            if (node != null) {
              compactionSet.remove(node);
              moveHTablesDown(node);

              if (node.isHtableLimitReached()) {
                compactionQueue.add(node);
                compactionSet.add(node);
              }
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
    System.out.println("Compaction is started");
    final MemTable[] memTables = new MemTable[8];
    for (int i = 0; i < memTables.length; i++) {
      memTables[i] = new MemTable(tableIdGen.getAndIncrement());
    }

    final List<HTable> hTables = node.getNOldestHTables(1024);
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

        final NodeN[] children = node.getChildren();
        for (int i = 0; i < children.length; i++) {
          final NodeN child = children[i];

          final MemTable memTable = memTables[i];
          if (memTable.isFilled()) {
            final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name);
            convert.invoke();

            final HTable newTable = convert.gethTable();
            child.addHTable(newTable, new HTableFile(convert.getBloomFilterPath(), convert.getHtablePath()));

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

    final NodeN[] children = node.getChildren();
    for (int i = 0; i < children.length; i++) {
      final NodeN child = children[i];
      final MemTable memTable = memTables[i];

      if (!memTable.isEmpty()) {
        final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name);
        convert.invoke();

        final HTable newTable = convert.gethTable();
        child.addHTable(newTable, new HTableFile(convert.getBloomFilterPath(), convert.getHtablePath()));
      }
    }

    for (HTable hTableToRemove : hTables) {
      node.removeTable(hTableToRemove.getId());
    }

    for (NodeN child : children) {
      if (child.isHtableLimitReached() && !compactionSet.contains(child)) {
        compactionQueue.add(child);
        compactionSet.add(child);
      }
    }
    System.out.println("Compaction is finished");
  }
}
