package com.orientechnologies.lsmtrie;

class HashUtils {
  public static long generateWaterMarkHash(byte[] sha1) {
    final long waterMark = ((0xFFL & sha1[8]) | ((0xFFL & sha1[9]) << 8) | ((0xFFL & sha1[10]) << 16) | ((0xFFL & sha1[11]) << 24));
    assert waterMark >= 0;
    return waterMark;
  }

  public static int bucketIndex(byte[] sha1) {
    int hashcode = 1;
    for (int i = 0; i < 8; i++) {
      hashcode = 31 * hashcode + sha1[i];
    }

    hashcode = hashcode ^ (hashcode >>> 16);

    return hashcode & (Table.BUCKETS_COUNT - 1);
  }

  public static int childNodeIndex(int level, byte[] sha1) {
    final int offset = (level - 1) * 3;//3 bits per level
    final int bytes = offset / 8;

    final int bitsOffset = offset - bytes * 8;
    if (bitsOffset < 6) {
      return ((0xFF & sha1[sha1.length - bytes - 1]) >>> (5 - bitsOffset)) & 0x7;
    } else {
      final int firstOffset = 8 - bitsOffset;
      final int firstPart =  ((0xFF & sha1[sha1.length - bytes - 1]) & (0x7 >>> (3 - firstOffset))) << (3 - firstOffset);
      final int secondPart = (0xFF & sha1[sha1.length - bytes - 2]) >>> (8 - (3 - firstOffset));
      return firstPart | secondPart;
    }
  }
}
