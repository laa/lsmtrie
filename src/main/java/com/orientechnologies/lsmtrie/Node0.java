package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Node0 implements Node {
  private final long       id;
  private final AtomicLong nodeIdGen;
  private final Semaphore  compactionCounter;

  private final ConcurrentSkipListMap<Long, AtomicReference<Table>> tables     = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<Long, HTableFile>                 tableFiles = new ConcurrentHashMap<>();
  private final AtomicReference<NodeN[]>                            children   = new AtomicReference<>();

  Node0(long id, AtomicLong nodeIdGen, Semaphore compactionCounter) {
    this.id = id;
    this.nodeIdGen = nodeIdGen;
    this.compactionCounter = compactionCounter;
  }

  public void addMemTable(MemTable table) {
    tables.put(table.getId(), new AtomicReference<>(table));
  }

  public void updateTable(HTable table, HTableFile hTableFile) {
    tableFiles.put(table.getId(), hTableFile);

    final AtomicReference<Table> tableRef = tables.get(table.getId());
    assert tableRef != null;
    tableRef.set(table);

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
    return 0;
  }

  @Override
  public NodeMetadata getMetadata() {
    final List<Long> tableIdList = new ArrayList<>();

    for (Map.Entry<Long, AtomicReference<Table>> entry : tables.entrySet()) {
      final AtomicReference<Table> tableRef = entry.getValue();
      final Table table = tableRef.get();
      if (table instanceof HTable) {
        tableIdList.add(entry.getKey());
      }
    }

    final String[] bloomFilterFiles = new String[tableIdList.size()];
    final String[] htableFiles = new String[tableIdList.size()];

    int counter = 0;
    for (Long nodeId : tableIdList) {
      final HTableFile hTableFile = tableFiles.get(nodeId);
      bloomFilterFiles[counter] = hTableFile.getBloomFilterPath().toAbsolutePath().toString();
      htableFiles[counter] = hTableFile.getHtablePath().toAbsolutePath().toString();
      counter++;
    }

    final long[] tableIds = new long[tableIdList.size()];
    for (int i = 0; i < tableIds.length; i++) {
      tableIds[i] = tableIdList.get(i);
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

    if (child != null) {
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
  public void addHTable(HTable hTable, HTableFile hTableFile) {
    final AtomicReference<Table> tableRef = new AtomicReference<>(hTable);
    tableFiles.put(hTable.getId(), hTableFile);
    tables.put(hTable.getId(), tableRef);

    compactionCounter.release(1);
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
