package com.orientechnologies.lsmtrie;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class NodeN implements Node {
  private final ConcurrentSkipListMap<Long, HTable> tables     = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<Long, HTableFile> tableFiles = new ConcurrentHashMap<>();
  private final AtomicReference<NodeN[]>            children   = new AtomicReference<>();

  private final int        level;
  private final long       id;
  private final AtomicLong nodeIdGen;

  NodeN(int level, long id, AtomicLong nodeIdGen) {
    this.level = level;
    this.id = id;
    this.nodeIdGen = nodeIdGen;
  }

  public void addHTable(HTable hTable, HTableFile hTableFile) {
    tables.put(hTable.getId(), hTable);
    tableFiles.put(hTable.getId(), hTableFile);
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
    final Iterator<HTable> values = tables.values().iterator();

    while (values.hasNext() && result.size() < limit) {
      final HTable hTable = values.next();
      result.add(hTable);
    }
    return result;
  }

  @Override
  public void removeTable(long id) {
    HTable hTable = tables.remove(id);
    hTable.clearBuffer();

    final HTableFile hTableFile = tableFiles.remove(id);
    try {
      Files.delete(hTableFile.getHtablePath());
      Files.delete(hTableFile.getBloomFilterPath());
    } catch (IOException e) {
      throw new IllegalStateException("Error during deletion of htable");
    }
  }

  public void delete() {
    for (HTable table : tables.values()) {
      table.clearBuffer();
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

  public int getLevel() {
    return level;
  }

  @Override
  public NodeMetadata getMetadata() {
    final List<Long> tableIdsList = new ArrayList<>(tables.keySet());

    final String[] bloomFiles = new String[tableIdsList.size()];
    final String[] htableFiles = new String[tableIdsList.size()];

    int counter = 0;
    for (Long tableId : tableIdsList) {
      final HTableFile file = tableFiles.get(tableId);

      bloomFiles[counter] = file.getBloomFilterPath().toAbsolutePath().toString();
      htableFiles[counter] = file.getHtablePath().toAbsolutePath().toString();
      counter++;
    }

    final long[] tableIds = new long[tableIdsList.size()];
    for (int i = 0; i < tableIds.length; i++) {
      tableIds[i] = tableIdsList.get(i);
    }

    return new NodeMetadata(id, level, bloomFiles, htableFiles, tableIds);
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
  public boolean hasChildren() {
    return children.get() != null;
  }

  public boolean isHtableLimitReached() {
    return tables.size() >= 8;
  }
}

