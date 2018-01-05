package com.orientechnologies.lsmtrie;

import java.util.List;

public interface Node {
  NodeN[] getChildren();

  boolean hasChildren();

  List<HTable> getNOldestHTables(int limit);

  void removeTable(long id);

  int getLevel();

  NodeMetadata getMetadata();

  void setChild(int index, NodeN child);

  void addHTable(HTable hTable);
}
