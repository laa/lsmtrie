package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class OLSMTrie {
  private static final ForkJoinPool compactionPool = ForkJoinPool.commonPool();

  private final    Path          root;
  private volatile MemTable      current;
  private final    String        name;
  private final    AtomicLong    tableIdGen            = new AtomicLong();
  private final    AtomicLong    nodeIdGen             = new AtomicLong();
  private final    Semaphore     allowedQueueMemtables = new Semaphore(4);
  private volatile Node0         node0;
  private final    AtomicBoolean stopCompaction        = new AtomicBoolean();
  private final    Registry      registry;

  private final Lock memtableAdditionLock = new ReentrantLock();

  private final    Semaphore      compactionCounter = new Semaphore(0);
  private volatile CompactionTask compactionTask;

  private final ExecutorService serviceThreads = Executors.newCachedThreadPool();

  OLSMTrie(String name, Path root) {
    this.name = name;
    this.root = root;
    this.registry = new Registry(root, name);
  }

  @SuppressWarnings("WeakerAccess")
  public void load() throws IOException {
    Files.createDirectories(root);

    node0 = registry.load(nodeIdGen, tableIdGen, compactionCounter);

    final long tableId = tableIdGen.getAndIncrement();
    final MemTable table = new MemTable(tableId);

    current = table;

    node0.addMemTable(table);

    compactionTask = new CompactionTask(name, compactionCounter, stopCompaction, node0, tableIdGen, root);
    compactionPool.submit(compactionTask);
  }

  @SuppressWarnings("WeakerAccess")
  public void close() {
    System.out.println("Conver memtable to htable");
    final MemTable memTable = current;
    memTable.waitTillZeroModifiers();

    if (memTable.isNotEmpty()) {
      final ConvertToHTableAction convertToHTableAction = new ConvertToHTableAction(memTable, root, name, 0);
      try {
        convertToHTableAction.invoke();
      } catch (IOException e) {
        throw new IllegalStateException("Can not convert memtable to htable", e);
      }

      final HTable hTable = convertToHTableAction.gethTable();
      node0.updateTable(hTable);
    }

    System.out.println("Wait till compaction is completed");

    while (compactionCounter.availablePermits() >= 8) {
      Thread.yield();
    }

    stopCompaction.set(true);
    try {
      compactionTask.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      throw new IllegalStateException("Abnormal termintation of compaction task", e);
    }

    serviceThreads.shutdown();

    try {
      if (!serviceThreads.awaitTermination(1, TimeUnit.HOURS)) {
        throw new IllegalStateException("Can not terminate service threads");
      }
    } catch (InterruptedException e) {
      return;
    }

    if (!compactionPool.awaitQuiescence(1, TimeUnit.HOURS)) {
      throw new IllegalStateException("Can not terminate compaction threads");
    }

    System.out.println("Compaction is completed");
    try {
      registry.save(node0);
      node0.close();
      registry.close();
    } catch (IOException e) {
      throw new IllegalStateException("Can not save trei state", e);
    }
  }

  @SuppressWarnings("WeakerAccess")
  public void delete() {
    stopCompaction.set(true);
    serviceThreads.shutdown();
    try {
      if (!serviceThreads.awaitTermination(1, TimeUnit.HOURS)) {
        throw new IllegalStateException("Can not terminate service threads");
      }
    } catch (InterruptedException e) {
      return;
    }

    node0.delete();
    try {
      registry.delete();
    } catch (IOException e) {
      throw new IllegalStateException("Can not delete trei configuration", e);
    }
  }

  public void put(byte[] key, byte[] value) {
    final MessageDigest digest = MessageDigestHolder.instance().get();
    digest.reset();

    final byte[] sha1 = digest.digest(key);

    MemTable memTable = current;
    boolean added = memTable.put(sha1, key, value);

    while (!added) {
      if (memtableAdditionLock.tryLock()) {
        try {
          if (memTable == current) {
            final MemTable newTable = new MemTable(tableIdGen.getAndIncrement());
            node0.addMemTable(newTable);
            current = newTable;

            try {
              allowedQueueMemtables.acquire();
            } catch (InterruptedException e) {
              return;
            }

            serviceThreads.submit(new MemTableSaver(memTable));
          }
        } finally {
          memtableAdditionLock.unlock();
        }
      }

      memTable = current;
      added = memTable.put(sha1, key, value);
    }
  }

  public byte[] get(byte[] key) {
    final MessageDigest digest = MessageDigestHolder.instance().get();
    digest.reset();

    final byte[] sha1 = digest.digest(key);

    return node0.get(key, sha1);
  }

  private class MemTableSaver implements Callable<Void> {
    private final MemTable memTable;

    MemTableSaver(MemTable memTable) {
      this.memTable = memTable;
    }

    @Override
    public Void call() throws Exception {
      try {
        memTable.waitTillZeroModifiers();

        final ConvertToHTableAction convertToHTableAction = new ConvertToHTableAction(memTable, root, name, 0).invoke();
        final HTable hTable = convertToHTableAction.gethTable();

        node0.updateTable(hTable);
        registry.save(node0);

        allowedQueueMemtables.release();
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }

  }

}
