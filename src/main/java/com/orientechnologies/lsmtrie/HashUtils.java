package com.orientechnologies.lsmtrie;

class HashUtils {
  public static long generateWaterMarkHash(byte[] sha1) {
    final long waterMark = ((0xFFL & sha1[8]) | ((0xFFL & sha1[9]) << 8) | ((0xFFL & sha1[10]) << 16) | ((0xFFL & sha1[11]) << 24));
    assert waterMark >= 0;
    return waterMark;
  }

  public static int getBucketIndex(byte[] sha1) {
    int hashcode = 1;
    for (int i = 0; i < 8; i++) {
      hashcode = 31 * hashcode + sha1[i];
    }

    hashcode = hashcode ^ (hashcode >>> 16);

    return hashcode & (Table.BUCKETS_COUNT - 1);
  }
}
