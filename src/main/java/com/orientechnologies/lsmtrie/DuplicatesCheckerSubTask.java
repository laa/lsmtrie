package com.orientechnologies.lsmtrie;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RecursiveAction;

public class DuplicatesCheckerSubTask extends RecursiveAction {
  private final List<HTable> hTables;
  private final Set<SHA1Holder> sha1Set = new HashSet<>();

  private final int bucketIndex;

  private int totalItems = 0;

  DuplicatesCheckerSubTask(List<HTable> hTables, int bucketIndex) {
    this.hTables = hTables;
    this.bucketIndex = bucketIndex;
  }

  @Override
  protected void compute() {
    for (HTable hTable : hTables) {
      final int bucketLength = hTable.bucketLength(bucketIndex);
      totalItems += bucketLength;

      for (int i = 0; i < bucketLength; i++) {
        final byte[] sha1 = hTable.getSHA(bucketIndex, i);
        sha1Set.add(new SHA1Holder(sha1));
      }
    }
  }

  public int getAllItems() {
    return totalItems;
  }

  public int getUniqueItems() {
    return sha1Set.size();
  }

  private static class SHA1Holder {
    private final byte[] sha1;

    private SHA1Holder(byte[] sha1) {
      this.sha1 = sha1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      SHA1Holder that = (SHA1Holder) o;
      return Arrays.equals(sha1, that.sha1);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(sha1);
    }
  }
}
