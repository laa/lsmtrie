package com.orientechnologies.lsmtrie;

public class NodeMetadata {
  private final long id;

  private final int      level;
  private final String[] bloomFilterPaths;
  private final String[] htablePaths;
  private final long[] tableIds;

  public NodeMetadata(long id, int level, String[] bloomFilterPaths, String[] htablePaths, long[] tableIds) {
    this.id = id;
    this.level = level;
    this.bloomFilterPaths = bloomFilterPaths;
    this.htablePaths = htablePaths;
    this.tableIds = tableIds;
  }

  public long getId() {
    return id;
  }

  public int getLevel() {
    return level;
  }

  public String[] getBloomFilterPaths() {
    return bloomFilterPaths;
  }

  public String[] getHtablePaths() {
    return htablePaths;
  }

  public long[] getTableIds() {
    return tableIds;
  }
}
