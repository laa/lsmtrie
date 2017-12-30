package com.orientechnologies.lsmtrie;

public class OHashUtils {
  public static long generateWaterMarkHash(byte[] sha1) {
    final long waterMark =
        ((0xFFL & sha1[2]) >>> 2) | ((0xFFL & sha1[3]) << 6) | ((0xFFL & sha1[4]) << 14) | ((0xFFL & sha1[5]) << 22) | ((0x3L & sha1[6])
            << 30);
    assert waterMark >= 0;
    return waterMark;

  }
}
