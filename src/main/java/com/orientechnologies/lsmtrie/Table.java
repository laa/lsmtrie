package com.orientechnologies.lsmtrie;

public interface Table {
  int SEGMENT_SIZE = 8 * 1024 * 1024;
  int TOTAL_SIZE   = 64 * 1024 * 1024;

  int BUCKET_SIZE        = 4 * 1024;
  int BUCKETS_COUNT      = 2 * 1024;
  int SHA_1_SIZE         = 20;
  int DATA_OFFSET_SIZE   = 4;
  int ENTRY_SIZE         = SHA_1_SIZE + DATA_OFFSET_SIZE;
  int DEST_BUCKET_OFFSET = 10;
  int WATER_MARK_OFFSET  = 8;

  int BUCKET_ENTRIES_COUNT_SIZE = 2;
  int DEST_BUCKET_SIZE          = 2;
  int WATER_MARK_SIZE           = 8;

  int BUCKET_DATA_SIZE_LIMIT = BUCKET_SIZE - (BUCKET_ENTRIES_COUNT_SIZE + DEST_BUCKET_SIZE + WATER_MARK_SIZE);

  int EMBEDDED_ENTREE_TYPE    = 1;
  int HEAP_ENTREE_TYPE        = 2;
  int EMBEDDED_TOMBSTONE_TYPE = 3;
  int SHA_TOMBSTONE_TOMBSTONE = 4;

  int ENTRY_TYPE_LENGTH = 1;
  int KEY_LENGTH        = 1;
  int VALUE_LENGTH      = 1;

  int HEAP_KEY_LENGTH   = 2;
  int HEAP_VALUE_LENGTH = 2;

  int HEAP_REFERENCE_LENGTH = 4;

  byte[] get(byte[] key, byte[] sha1);

  long getId();
}
