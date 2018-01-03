package com.orientechnologies.lsmtrie;

public interface Node {
  NodeN[] getChildren();

  HTable getOldestHtable();

  void removeTable(long id);

  int getLevel();
}
