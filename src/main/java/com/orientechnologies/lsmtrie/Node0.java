package com.orientechnologies.lsmtrie;

import com.sun.jna.Platform;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Node0 implements Node {
  private final int        level;
  private final long       id;
  private final AtomicLong idGen;
  private final Semaphore  compactionCounter;

  private final ConcurrentSkipListMap<Long, AtomicReference<Table>> tables     = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<Long, HTableFile>                 tableFiles = new ConcurrentHashMap<>();
  private final AtomicReference<NodeN[]>                            children   = new AtomicReference<>();

  Node0(int level, long id, AtomicLong idGen, Semaphore compactionCounter) {
    this.level = level;
    this.id = id;
    this.idGen = idGen;
    this.compactionCounter = compactionCounter;
  }

  public void addMemTable(MemTable table) {
    tables.put(table.getId(), new AtomicReference<>(table));
  }

  public void updateTable(HTable table, HTableFile hTableFile) {
    final AtomicReference<Table> tableRef = tables.get(table.getId());
    assert tableRef != null;
    tableRef.set(table);

    tableFiles.put(table.getId(), hTableFile);
    compactionCounter.release(1);
  }

  public void removeTable(long id) {
    final AtomicReference<Table> table = tables.remove(id);
    if (table.get() instanceof HTable) {
      ((HTable) table.get()).clearBuffer();
    }

    final HTableFile hTableFile = tableFiles.remove(id);
    if (hTableFile != null) {
      try {
        Files.delete(hTableFile.getHtablePath());
        Files.delete(hTableFile.getBloomFilterPath());
      } catch (IOException e) {
        throw new IllegalStateException("Error during deletion of htable", e);
      }
    }
  }

  public byte[] get(byte[] key, byte[] sha1) {
    for (AtomicReference<Table> tableRef : tables.values()) {
      final Table table = tableRef.get();
      final byte[] value = table.get(key, sha1);
      if (value != null) {
        return value;
      }
    }

    final NodeN[] children = this.children.get();
    if (children != null) {
      final NodeN child = children[HashUtils.childNodeIndex(level + 1, sha1)];
      return child.get(key, sha1);
    }

    return null;
  }

  public NodeN[] getChildren() {
    final NodeN[] current = children.get();
    if (current != null) {
      return current;
    }

    NodeN[] children = new NodeN[8];
    for (int i = 0; i < children.length; i++) {
      children[i] = new NodeN(level + 1, idGen.getAndIncrement(), idGen);
    }

    if (this.children.compareAndSet(null, children)) {
      return children;
    }

    return this.children.get();
  }

  public void close() {
    for (AtomicReference<Table> hTableRef : tables.values()) {
      final Table table = hTableRef.get();
      if (table instanceof HTable) {
        ((HTable) table).clearBuffer();
      }
    }

    final NodeN[] children = this.children.get();
    if (children != null) {
      for (NodeN child : children) {
        child.close();
      }
    }
  }

  public List<HTable> getNOldestHTables(int limit) {
    final List<HTable> result = new ArrayList<>();
    final Iterator<AtomicReference<Table>> values = tables.values().iterator();

    while (values.hasNext() && result.size() < limit) {
      final AtomicReference<Table> tableRef = values.next();
      final Table table = tableRef.get();
      if (table instanceof MemTable) {
        break;
      }

      result.add((HTable) table);
    }

    return result;
  }

  @Override
  public int getLevel() {
    return level;
  }

  public void delete() {
    for (AtomicReference<Table> hTableRef : tables.values()) {
      final Table table = hTableRef.get();
      if (table instanceof HTable) {
        ((HTable) table).clearBuffer();
      }
    }

    for (HTableFile hTableFile : tableFiles.values()) {
      try {
        Files.delete(hTableFile.getBloomFilterPath());
        Files.delete(hTableFile.getHtablePath());
      } catch (IOException e) {
        throw new IllegalStateException("Error during deletion of htable", e);
      }
    }

    final NodeN[] children = this.children.get();
    if (children != null) {
      for (NodeN child : children) {
        child.delete();
      }
    }
  }
}
