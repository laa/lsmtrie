package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
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

  private final ConcurrentSkipListMap<Long, AtomicReference<Table>> tables        = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<Long, HTableFileChannel>          tableChannels = new ConcurrentHashMap<>();
  private final AtomicReference<NodeN[]>                            children      = new AtomicReference<>();

  Node0(int level, long id, AtomicLong idGen, Semaphore compactionCounter) {
    this.level = level;
    this.id = id;
    this.idGen = idGen;
    this.compactionCounter = compactionCounter;
  }

  public void addMemTable(MemTable table) {
    tables.put(table.getId(), new AtomicReference<>(table));
  }

  public void updateTable(HTable table, HTableFileChannel hTableFileChannel) {
    final AtomicReference<Table> tableRef = tables.get(table.getId());
    assert tableRef != null;
    tableRef.set(table);

    tableChannels.put(table.getId(), hTableFileChannel);
    compactionCounter.release(1);
  }

  public void removeTable(long id) {
    tables.remove(id);
    final HTableFileChannel hTableFileChannel = tableChannels.get(id);
    if (hTableFileChannel != null) {
      try {
        hTableFileChannel.getChannel().close();
        Files.delete(hTableFileChannel.getHtablePath());
        Files.delete(hTableFileChannel.getBloomFilterPath());
      } catch (IOException e) {
        throw new IllegalStateException("Error during deletion of htable");
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
    for (HTableFileChannel hTableFileChannel : tableChannels.values()) {
      try {
        hTableFileChannel.getChannel().close();
      } catch (IOException e) {
        throw new IllegalStateException("Error during channel close", e);
      }
    }

    final NodeN[] children = this.children.get();
    if (children != null) {
      for (NodeN child : children) {
        child.close();
      }
    }
  }

  public HTable getOldestHtable() {
    final Map.Entry<Long, AtomicReference<Table>> entry = tables.lastEntry();
    if (entry == null) {
      return null;
    }

    final AtomicReference<Table> tableRef = entry.getValue();
    final Table table = tableRef.get();
    if (table instanceof HTable) {
      return (HTable) table;
    }

    return null;
  }

  @Override
  public int getLevel() {
    return level;
  }

  public void delete() {
    for (HTableFileChannel hTableFileChannel : tableChannels.values()) {
      try {
        hTableFileChannel.getChannel().close();
        Files.delete(hTableFileChannel.getBloomFilterPath());
        Files.delete(hTableFileChannel.getHtablePath());
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
