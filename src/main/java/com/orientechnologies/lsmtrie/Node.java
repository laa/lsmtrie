package com.orientechnologies.lsmtrie;

import java.util.List;

public interface Node {
  NodeN[] getChildren();

  List<HTable> getNOldestHTables(int limit);

  void removeTable(long id);

  int getLevel();
}
