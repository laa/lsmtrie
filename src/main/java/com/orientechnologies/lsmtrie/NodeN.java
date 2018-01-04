package com.orientechnologies.lsmtrie;

import com.sun.jna.Platform;

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
  private final AtomicLong idGen;

  NodeN(int level, long id, AtomicLong idGen) {
    this.level = level;
    this.id = id;
    this.idGen = idGen;
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
      children[i] = new NodeN(level + 1, idGen.getAndIncrement(), idGen);
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

  public boolean isHtableLimitReached() {
    return tables.size() >= 8;
  }
}

