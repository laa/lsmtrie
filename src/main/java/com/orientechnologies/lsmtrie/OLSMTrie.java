package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

public class OLSMTrie {
  private static final ForkJoinPool compactionPool = ForkJoinPool.commonPool();

  private final Path root;
  private final AtomicReference<MemTable> current = new AtomicReference<>();
  private final String name;
  private final AtomicLong tableIdGen            = new AtomicLong();
  private final AtomicLong nodeIdGen             = new AtomicLong();
  private final Semaphore  allowedQueueMemtables = new Semaphore(2);
  private volatile Node0 node0;
  private final AtomicBoolean stopCompaction = new AtomicBoolean();
  private final Registry registry;

  private final Semaphore compactionCounter = new Semaphore(0);
  private volatile CompactionTask compactionTask;

  private final ExecutorService serviceThreads = Executors.newCachedThreadPool();

  private final ThreadLocal<MessageDigest> messageDigest = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 algorithm is not implemented", e);
    }
  });

  public OLSMTrie(String name, Path root) {
    this.name = name;
    this.root = root;
    this.registry = new Registry(root, name);
  }

  public void load() throws IOException {
    Files.createDirectories(root);

    node0 = registry.load(nodeIdGen, tableIdGen, compactionCounter);

    final long tableId = tableIdGen.getAndIncrement();
    final MemTable table = new MemTable(tableId);

    current.set(table);

    node0.addMemTable(table);

    compactionTask = new CompactionTask(name, compactionCounter, stopCompaction, node0, tableIdGen, root);
    compactionPool.submit(compactionTask);
  }

  public void close() {
    System.out.println("Conver memtable to htable");
    final MemTable memTable = current.get();
    memTable.waitTillZeroModifiers();

    if (!memTable.isEmpty()) {
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
    final MessageDigest digest = messageDigest.get();
    digest.reset();

    final byte[] sha1 = digest.digest(key);

    MemTable memTable = current.get();
    boolean added = memTable.put(sha1, key, value);

    while (!added) {
      final MemTable newTable = new MemTable(tableIdGen.getAndIncrement());
      node0.addMemTable(newTable);
      if (current.compareAndSet(memTable, newTable)) {
        try {
          allowedQueueMemtables.acquire();
        } catch (InterruptedException e) {
          return;
        }
        serviceThreads.submit(new MemTableSaver(memTable));
      } else {
        node0.removeTable(newTable.getId());
      }

      memTable = current.get();
      added = memTable.put(sha1, key, value);
    }
  }

  public byte[] get(byte[] key) {
    final MessageDigest digest = messageDigest.get();
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
