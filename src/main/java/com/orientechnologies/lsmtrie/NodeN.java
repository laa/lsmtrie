package com.orientechnologies.lsmtrie;

import com.sun.jna.Platform;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class NodeN implements Node {
  private final ConcurrentSkipListMap<Long, HTable>        tables        = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<Long, HTableFileChannel> tableChannels = new ConcurrentHashMap<>();
  private final AtomicReference<NodeN[]>                   children      = new AtomicReference<>();

  private final int        level;
  private final long       id;
  private final AtomicLong idGen;

  NodeN(int level, long id, AtomicLong idGen) {
    this.level = level;
    this.id = id;
    this.idGen = idGen;
  }

  public void addHTable(HTable hTable, HTableFileChannel hTableFileChannel) {
    tables.put(hTable.getId(), hTable);
    tableChannels.put(hTable.getId(), hTableFileChannel);
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
    for (HTableFileChannel hTableFileChannel : tableChannels.values()) {
      try {
        hTableFileChannel.getChannel().close();
      } catch (IOException e) {
        throw new IllegalStateException("Can not close file channel", e);
      }
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
  public List<HTable> getNOldestHTables(int n) {
    final List<HTable> result = new ArrayList<>();
    final Iterator<HTable> entries = tables.values().iterator();
    int counter = 0;

    while (entries.hasNext() && counter < n) {
      final HTable table = entries.next();
      result.add(table);
    }

    return result;

  }

  @Override
  public void removeTable(long id) {
    tables.remove(id);
    final HTableFileChannel hTableFileChannel = tableChannels.remove(id);
    try {
      hTableFileChannel.getChannel().close();
      try {
        Files.delete(hTableFileChannel.getHtablePath());
      } catch (IOException e) {
        if (Platform.isWindows()) {
          System.out.println("Can not delete htable file on windows");
        }
      }

      Files.delete(hTableFileChannel.getBloomFilterPath());
    } catch (IOException e) {
      throw new IllegalStateException("Error during deletion of htable");
    }
  }

  public void delete() {
    for (HTableFileChannel hTableFileChannel : tableChannels.values()) {
      try {
        hTableFileChannel.getChannel().close();
        Files.delete(hTableFileChannel.getBloomFilterPath());
        try {
          Files.delete(hTableFileChannel.getHtablePath());
        } catch (IOException e) {
          if (Platform.isWindows()) {
            System.out.println("Can not delete htable file on windows");
          }
        }

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

