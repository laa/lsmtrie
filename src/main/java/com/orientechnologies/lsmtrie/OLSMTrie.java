package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class OLSMTrie {
  private final Set<FileChannel> htableChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final String name;
  private final AtomicLong                                          tableIdGen            = new AtomicLong();
  private final ConcurrentSkipListMap<Long, AtomicReference<Table>> tables                = new ConcurrentSkipListMap<>();
  private final AtomicReference<MemTable>                           current               = new AtomicReference<>();
  private final Semaphore                                           allowedQueueMemtables = new Semaphore(2);
  private final ExecutorService                                     serviceThreads        = Executors.newCachedThreadPool();

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
    tables.put(tableId, new AtomicReference<>(table));
  }

  public void close() {
    serviceThreads.shutdown();
    try {
      if (!serviceThreads.awaitTermination(1, TimeUnit.HOURS)) {
        throw new IllegalStateException("Can not terminate service threads");
      }
    } catch (InterruptedException e) {
      return;
    }

    for (FileChannel channel : htableChannels) {
      try {
        channel.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void put(byte[] key, byte[] value) {
    final MessageDigest digest = messageDigest.get();
    digest.reset();

    final byte[] sha1 = digest.digest(key);

    final MemTable memTable = current.get();
    final boolean added = memTable.put(sha1, key, value);

    if (!added) {
      final MemTable newTable = new MemTable(tableIdGen.getAndIncrement());
      tables.put(newTable.getId(), new AtomicReference<>(newTable));
      if (current.compareAndSet(memTable, newTable)) {
        try {
          allowedQueueMemtables.acquire();
        } catch (InterruptedException e) {
          return;
        }
        serviceThreads.submit(new MemTableSaver(memTable));
      } else {
        tables.remove(newTable.getId());
      }
    }
  }

  public byte[] get(byte[] key) {
    final MessageDigest digest = messageDigest.get();
    digest.reset();

    byte[] value = null;
    final byte[] sha1 = digest.digest(key);
    for (AtomicReference<Table> tableRef : tables.values()) {
      final Table table = tableRef.get();
      final byte[] tableValue = table.get(key, sha1);
      if (tableValue != null) {
        value = tableValue;
      }
    }

    return value;
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

        final SerializedHTable serializedHTable = memTable.toHTable();
        final long tableId = memTable.getId();

        final String htableName = name + "_" + tableId + ".htb";
        final String bloomFiltersName = name + "_" + tableId + ".bl";

        final FileChannel bloomChannel = FileChannel
            .open(Paths.get(bloomFiltersName), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);

        try (final OutputStream outputStream = Channels.newOutputStream(bloomChannel)) {
          for (BloomFilter<byte[]> bloomFilter : serializedHTable.getBloomFilters()) {
            bloomFilter.writeTo(outputStream);
          }
        }
        bloomChannel.force(true);
        bloomChannel.close();

        final FileChannel htableChannel = FileChannel
            .open(Paths.get(htableName), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
        htableChannels.add(htableChannel);
        htableChannel.write(serializedHTable.getHtableBuffer());
        serializedHTable.free();
        htableChannel.force(true);

        final HTable hTable = new HTable(serializedHTable.getBloomFilters(),
            htableChannel.map(FileChannel.MapMode.READ_ONLY, 0, serializedHTable.getHtableSize()), tableId);
        final AtomicReference<Table> table = tables.get(tableId);
        table.set(hTable);

        allowedQueueMemtables.release();
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }
}
