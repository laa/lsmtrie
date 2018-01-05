package com.orientechnologies.lsmtrie;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class HashUtilsTest {
  @Test
  public void nodeIndexTest() {
    final long seed = System.nanoTime();
    System.out.println("HashUtilsTest.nodeIndexTest seed: " + seed);

    for (int i =0; i <100000; i++) {
      final Random random = new Random(seed);

      final int code = random.nextInt();
      for (int level = 1; level <= 10; level++) {
        final int index = (code >>> (32 - level * 3)) & 0x7;

        final ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(code);

        Assert.assertEquals(index, HashUtils.childNodeIndex(level, buffer.array()));
      }
    }
  }
}
