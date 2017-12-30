package com.orientechnologies.lsmtrie;

public interface OTable {
  int BUCKET_SIZE          = 4 * 1024;
  int BUCKETS_COUNT        = 1024;
  int SHA_1_SIZE           = 20;
  int DATA_OFFSET_SIZE     = 4;
  int ENTRY_SIZE           = SHA_1_SIZE + DATA_OFFSET_SIZE;
  int BUCKET_ENTRIES_COUNT = BUCKET_SIZE / ENTRY_SIZE;
  int DEST_BUCKET_OFFSET   = 12;
  int WATER_MARK_OFFSET    = 8;

  byte[] get(byte[] key, byte[] sha1);

  long getId();
}
