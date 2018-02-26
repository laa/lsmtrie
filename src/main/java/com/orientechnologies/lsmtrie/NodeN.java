package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public class NodeN implements Node {
  private final AtomicInteger                       tablesCounter = new AtomicInteger();
  private final ConcurrentSkipListMap<Long, HTable> tables        = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
  private final AtomicReference<NodeN[]>            children      = new AtomicReference<>();

  private final int        level;
  private final long       id;
  private final AtomicLong nodeIdGen;

  private int lastDuplicationCheck;

  NodeN(int level, long id, AtomicLong nodeIdGen) {
    this.level = level;
    this.id = id;
    this.nodeIdGen = nodeIdGen;
  }

  public void addHTable(HTable hTable) {
    tables.put(hTable.getId(), hTable);
    tablesCounter.getAndIncrement();
  }

  public byte[] get(byte[] key, byte[] sha1) {
    for (HTable hTable : tables.values()) {
      final byte[] value = hTable.get(key, sha1);
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

  public void close() {
    for (HTable hTable : tables.values()) {
      hTable.clearBuffer();
    }

    final NodeN[] children = this.children.get();
    if (children != null) {
      for (NodeN child : children) {
        child.close();
      }
    }
  }

  public int hTablesCount() {
    return tablesCounter.get();
  }

  @Override
  public NodeN[] getChildren() {
    final NodeN[] current = children.get();
    if (current != null) {
      return current;
    }

    NodeN[] children = new NodeN[8];
    for (int i = 0; i < children.length; i++) {
      children[i] = new NodeN(level + 1, nodeIdGen.getAndIncrement(), nodeIdGen);
    }

    if (this.children.compareAndSet(null, children)) {
      return children;
    }

    return this.children.get();
  }

  @Override
  public List<HTable> getNOldestHTables(int limit) {
    final List<HTable> result = new ArrayList<>();
    final Iterator<HTable> values = tables.descendingMap().values().iterator();

    while (values.hasNext() && result.size() < limit) {
      final HTable hTable = values.next();
      result.add(hTable);
    }

    return result;
  }

  public List<HTable> getHTables() {
    return new ArrayList<>(tables.values());
  }

  @Override
  public void removeTable(long id) {
    HTable hTable = tables.remove(id);
    hTable.waitTillReaders();

    hTable.clearBuffer();

    try {
      Files.delete(hTable.getHtablePath());
      Files.delete(hTable.getBloomFilterPath());
    } catch (IOException e) {
      throw new IllegalStateException("Error during deletion of htable");
    }

    tablesCounter.decrementAndGet();
  }

  public void delete() {
    for (HTable table : tables.values()) {
      table.clearBuffer();

      try {
        Files.delete(table.getBloomFilterPath());
        Files.delete(table.getHtablePath());
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

    tablesCounter.set(0);
  }

  public int getLevel() {
    return level;
  }

  @Override
  public NodeMetadata getMetadata() {
    final List<HTable> tableList = new ArrayList<>(tables.values());

    final String[] bloomFiles = new String[tableList.size()];
    final String[] htableFiles = new String[tableList.size()];
    final long[] tableIds = new long[tableList.size()];

    int counter = 0;
    for (HTable table : tableList) {
      bloomFiles[counter] = table.getBloomFilterPath().toAbsolutePath().toString();
      htableFiles[counter] = table.getHtablePath().toAbsolutePath().toString();
      tableIds[counter] = table.getId();
      counter++;
    }

    return new NodeMetadata(id, level, bloomFiles, htableFiles, tableIds);
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
  public boolean hasChildren() {
    return children.get() != null;
  }

  public int getLastDuplicationCheck() {
    return lastDuplicationCheck;
  }

  public void setLastDuplicationCheck(int lastDuplicationCheck) {
    this.lastDuplicationCheck = lastDuplicationCheck;
  }

}

