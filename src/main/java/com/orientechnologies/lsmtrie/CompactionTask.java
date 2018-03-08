package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
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
            moveHTablesDown(node0, 8);

            NodeN node;

            while ((node = compactionQueue.poll()) != null) {
              moveHTablesDown(node, 0);
            }
          } else {
            if (!stop.get()) {
              final Node notEmptyNode = findFirstNotEmptyNode(node0);
              if (notEmptyNode == null) {
                continue;
              }

              System.out.println("No overflown nodes found,but not empty node was found compaction is started from it");
              if (!stop.get()) {
                moveHTablesDown(notEmptyNode, 0);

                NodeN node;

                while ((node = compactionQueue.poll()) != null) {
                  moveHTablesDown(node, 0);
                }
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

  private Node findFirstNotEmptyNode(Node node) {
    if (!node.hasChildren()) {
      return null;
    }

    final List<HTable> hTables = node.getNOldestHTables(1);

    if (!hTables.isEmpty()) {
      return node;
    }

    final NodeN[] children = node.getChildren();
    for (NodeN child : children) {
      final Node notEmptyNode = findFirstNotEmptyNode(child);

      if (notEmptyNode != null) {
        return notEmptyNode;
      }
    }

    return null;
  }

  private void moveHTablesDown(Node node, int acquiredPermits) throws IOException {
    final long start = System.nanoTime();
    int newTables = 0;

    final List<HTable> hTables = node.getNOldestHTables(1024);
    System.out.printf("Compaction is started for node from level %d which contains %d htables\n", node.getLevel(), hTables.size());

    if (node instanceof Node0) {
      try {
        compactionCounter.acquire(hTables.size() - acquiredPermits);
      } catch (InterruptedException e) {
        return;
      }
    }

    final NodeN[] children = node.getChildren();

    final MemTable[] memTables = new MemTable[8];
    for (int i = 0; i < memTables.length; i++) {
      memTables[i] = new MemTable(tableIdGen.getAndIncrement());
    }

    final HTable[] childHTablesToRemove = new HTable[8];
    int childHTablesToRemoveCount = 0;

    for (int i = 0; i < children.length; i++) {
      final NodeN child = children[i];

      final HTable hTable = child.getLatestHTable();
      if (hTable != null) {
        childHTablesToRemove[i] = hTable;
        childHTablesToRemoveCount++;

        final List<BucketLoader> bucketLoaders = new ArrayList<>(Table.BUCKETS_COUNT);

        for (int n = 0; n < Table.BUCKETS_COUNT; n++) {
          bucketLoaders.add(new BucketLoader(n, hTable, memTables[i]));
        }

        ForkJoinTask.invokeAll(bucketLoaders);
      }
    }

    for (HTable hTable : hTables) {
      boolean isCompleted = false;

      List<CompactionSubTask> subTasks = new ArrayList<>(Table.BUCKETS_COUNT);
      for (int i = 0; i < Table.BUCKETS_COUNT; i++) {
        subTasks.add(new CompactionSubTask(memTables, hTable, i, node.getLevel() + 1));
      }

      while (!isCompleted) {
        ForkJoinTask.invokeAll(subTasks);

        for (int i = 0; i < children.length; i++) {
          final MemTable memTable = memTables[i];

          if (memTable.isFilled()) {
            final ConvertToHTableAction convert = new ConvertToHTableAction(memTable, root, name, node.getLevel() + 1);
            convert.invoke();

            final HTable newTable = convert.gethTable();
            newTables++;

            final NodeN child = children[i];
            child.addHTable(newTable);

            memTables[i] = new MemTable(tableIdGen.getAndIncrement());
          }
        }

        final List<CompactionSubTask> notCompletedTasks = new ArrayList<>();
        for (CompactionSubTask subTask : subTasks) {
          if (!subTask.isComplete()) {
            notCompletedTasks.add(subTask);
          }
        }

        isCompleted = notCompletedTasks.isEmpty();
        if (!isCompleted) {
          notCompletedTasks.forEach(ForkJoinTask::reinitialize);
          subTasks = notCompletedTasks;
        }
      }
    }

    for (int i = 0; i < children.length; i++) {
      final NodeN child = children[i];
      final MemTable memTable = memTables[i];

      if (memTable.isNotEmpty()) {
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

    for (int i = 0; i < children.length; i++) {
      final HTable hTable = childHTablesToRemove[i];

      if (hTable != null) {
        final NodeN child = children[i];
        child.removeTable(hTable.getId());
      }
    }

    for (int i = 0; i < 8; i++) {
      final NodeN child = children[i];
      final int hTablesCount = child.hTablesCount();

      if (hTablesCount >= 8) {
        compactionQueue.add(child);
      }
    }
    final long end = System.nanoTime();
    System.out.printf(
        "Compaction is finished in %d ms. ,%d tables were compacted, %d tables were created, %d child tables were removed\n",
        (end - start) / 1000_000, hTables.size(), newTables, childHTablesToRemoveCount);
  }

}
