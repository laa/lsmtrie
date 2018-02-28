package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Node0 implements Node {
  private final long       id;
  private final AtomicLong nodeIdGen;
  private final Semaphore  compactionCounter;

  private final ConcurrentSkipListMap<Long, AtomicReference<Table>> tables   = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
  private final AtomicReference<NodeN[]>                            children = new AtomicReference<>();

  Node0(long id, AtomicLong nodeIdGen, Semaphore compactionCounter) {
    this.id = id;
    this.nodeIdGen = nodeIdGen;
    this.compactionCounter = compactionCounter;
  }

  public void addMemTable(MemTable table) {
    tables.put(table.getId(), new AtomicReference<>(table));
  }

  public void updateTable(HTable table) {
    final AtomicReference<Table> tableRef = tables.get(table.getId());
    assert tableRef != null;

    tableRef.set(table);

    compactionCounter.release(1);
  }

  public void removeTable(long id) {
    final AtomicReference<Table> tableRef = tables.remove(id);
    final Table table = tableRef.get();
    if (table instanceof HTable) {
      final HTable hTable = (HTable) table;
      hTable.clearBuffer();

      try {
        Files.delete(hTable.getHtablePath());
        Files.delete(hTable.getBloomFilterPath());
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
      final NodeN child = children[HashUtils.childNodeIndex(1, sha1)];
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
      children[i] = new NodeN(1, nodeIdGen.getAndIncrement(), nodeIdGen);
    }

    if (this.children.compareAndSet(null, children)) {
      return children;
    }

    return this.children.get();
  }

  public void close() {
    for (AtomicReference<Table> tableRef : tables.values()) {
      final Table table = tableRef.get();

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
    final Iterator<AtomicReference<Table>> values = tables.descendingMap().values().iterator();

    while (values.hasNext() && result.size() < limit) {
      final AtomicReference<Table> tableRef = values.next();
      final Table table = tableRef.get();
      if (table instanceof MemTable) {
        break;
      }

      result.add(((HTable) table));
    }

    return result;
  }

  @Override
  public int getLevel() {
    return 0;
  }

  @Override
  public NodeMetadata getMetadata() {
    final List<HTable> tableList = new ArrayList<>();

    for (AtomicReference<Table> tableRef : tables.values()) {
      final Table table = tableRef.get();
      if (table instanceof HTable) {
        tableList.add((HTable) table);
      }
    }

    final long[] tableIds = new long[tableList.size()];
    final String[] bloomFilterFiles = new String[tableList.size()];
    final String[] htableFiles = new String[tableList.size()];

    int counter = 0;
    for (HTable hTable : tableList) {
      tableIds[counter] = hTable.getId();
      bloomFilterFiles[counter] = hTable.getBloomFilterPath().toAbsolutePath().toString();
      htableFiles[counter] = hTable.getHtablePath().toAbsolutePath().toString();
      counter++;
    }

    return new NodeMetadata(id, 0, bloomFilterFiles, htableFiles, tableIds);
  }

  @Override
  public boolean hasChildren() {
    return children.get() != null;
  }

  @Override
  public void setChild(int index, NodeN child) {
    final NodeN[] children = this.children.get();
    final NodeN[] newChildren = new NodeN[8];

    if (children != null) {
      System.arraycopy(children, 0, newChildren, 0, children.length);
    }

    if (newChildren[index] != null) {
      throw new IllegalStateException("Child with index " + index + " is already set");
    }

    newChildren[index] = child;
    if (!this.children.compareAndSet(children, newChildren)) {
      throw new IllegalStateException("Children of the node were concurrently updated");
    }
  }

  @Override
  public void addHTable(HTable hTable) {
    final AtomicReference<Table> tableRef = new AtomicReference<>(hTable);
    tables.put(hTable.getId(), tableRef);

    compactionCounter.release(1);
  }

  public void delete() {
    for (AtomicReference<Table> tableRef : tables.values()) {
      final Table table = tableRef.get();

      if (table instanceof HTable) {
        final HTable hTable = (HTable) table;
        hTable.clearBuffer();

        try {
          Files.delete(hTable.getBloomFilterPath());
          Files.delete(hTable.getHtablePath());
        } catch (IOException e) {
          throw new IllegalStateException("Error during deletion of htable", e);
        }
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
