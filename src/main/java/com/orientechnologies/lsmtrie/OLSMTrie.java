package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
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

  private final AtomicReference<MemTable> current = new AtomicReference<>();
  private final String name;
  private final AtomicLong tableIdGen            = new AtomicLong();
  private final AtomicLong nodeIdGen             = new AtomicLong();
  private final Semaphore  allowedQueueMemtables = new Semaphore(2);
  private final Node0          node0;
  private final CompactionTask compactionTask;
  private final AtomicBoolean stopCompaction = new AtomicBoolean();

  private final Semaphore compactionCounter = new Semaphore(0);

  private final ExecutorService serviceThreads = Executors.newCachedThreadPool();

  private final ThreadLocal<MessageDigest> messageDigest = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 algorithm is not implemented", e);
    }
  });

  public OLSMTrie(String name) {
    this.name = name;

    final long tableId = tableIdGen.getAndIncrement();
    final MemTable table = new MemTable(tableId);

    current.set(table);

    node0 = new Node0(0, nodeIdGen.getAndIncrement(), nodeIdGen, compactionCounter);
    node0.addMemTable(table);
    compactionTask = new CompactionTask(name, compactionCounter, stopCompaction, node0);
    compactionPool.submit(compactionTask);
  }

  public void close() {
    final MemTable memTable = current.get();
    memTable.waitTillZeroModifiers();

    final ConvertToHTableAction convertToHTableAction = new ConvertToHTableAction(memTable, name);
    try {
      convertToHTableAction.invoke();
    } catch (IOException e) {
      throw new IllegalStateException("Can not convert memtable to htable", e);
    }

    final Path htablePath = convertToHTableAction.getHtablePath();
    final Path bloomFilterPath = convertToHTableAction.getBloomFilterPath();
    final FileChannel htableChannel = convertToHTableAction.getHtableChannel();
    final HTable hTable = convertToHTableAction.gethTable();

    node0.updateTable(hTable, new HTableFileChannel(bloomFilterPath, htablePath, htableChannel));

    stopCompaction.set(true);
    serviceThreads.shutdown();
    try {
      if (!serviceThreads.awaitTermination(1, TimeUnit.HOURS)) {
        throw new IllegalStateException("Can not terminate service threads");
      }
    } catch (InterruptedException e) {
      return;
    }

    node0.close();
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
  }

  public void put(byte[] key, byte[] value) {
    final MessageDigest digest = messageDigest.get();
    digest.reset();

    final byte[] sha1 = digest.digest(key);

    final MemTable memTable = current.get();
    final boolean added = memTable.put(sha1, key, value);

    if (!added) {
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

        final ConvertToHTableAction convertToHTableAction = new ConvertToHTableAction(memTable, name).invoke();
        final Path htablePath = convertToHTableAction.getHtablePath();
        final Path bloomFilterPath = convertToHTableAction.getBloomFilterPath();
        final FileChannel htableChannel = convertToHTableAction.getHtableChannel();
        final HTable hTable = convertToHTableAction.gethTable();

        node0.updateTable(hTable, new HTableFileChannel(bloomFilterPath, htablePath, htableChannel));

        allowedQueueMemtables.release();
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }

  }

}
