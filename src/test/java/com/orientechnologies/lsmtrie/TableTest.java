package com.orientechnologies.lsmtrie;

import org.junit.Test;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableTest {
  @Test
  public void testAddNValues() throws Exception {
    for (int k = 0; k < 600; k++) {
      for (int i = 0; i < 18; i++) {
        final int items = 1 << i;

        final long seed = System.nanoTime();

        System.out.println("testAdd " + items + " items seed : " + seed);
        final Random random = new Random(seed);

        Map<ByteHolder, ByteHolder> entries = generateNEntries(items, random);
        assertEquals(items, entries.size());
        OMemTable memTable = new OMemTable(1);

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        long fillStart = System.nanoTime();
        fillMemTable(memTable, entries, digest);
        long fillEnd = System.nanoTime();

        Set<ByteHolder> nonExisting = generateNNotExistingEntries(items, entries, random);
        assertEquals(items, nonExisting.size());

        long assertMemTableStart = System.nanoTime();
        assertTable(entries, nonExisting, memTable, digest);
        long assertMemTableEnd = System.nanoTime();

        long toHTableStart = System.nanoTime();
        SerializedHTable serializedHTable = memTable.toHTable();
        long toHTableEnd = System.nanoTime();

        final OHTable hTable = new OHTable(serializedHTable.getBloomFilters(),
            serializedHTable.getHtableBuffer().asReadOnlyBuffer(), 1);

        long assertHTableStart = System.nanoTime();
        assertTable(entries, nonExisting, hTable, digest);
        long assertHTableEnd = System.nanoTime();

        serializedHTable.free();
        if (i == 17) {
          System.out
              .printf("Fill time %d ns., assert memtable time %d ns., htable conversion time %d ns., assert htable time %d ns.\n",
                  fillEnd - fillStart, assertMemTableEnd - assertMemTableStart, toHTableEnd - toHTableStart,
                  assertHTableEnd - assertHTableStart);
        }
      }
    }
  }

  @Test
  public void testTillFull() throws Exception {
    long assertHTableTime = 0;
    long convertHTableTime = 0;
    int counter = 0;

    for (int k = 0; k < 3600; k++) {
      final Map<ByteHolder, ByteHolder> data = new HashMap<>();
      final OMemTable memTable = new OMemTable(1);

      final long seed = System.nanoTime();
      System.out.println("testTillFull seed : " + seed);
      Random random = new Random(seed);

      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      byte[] key;
      byte[] value;
      byte[] sha1;
      boolean added;
      do {
        key = generateKey(random);

        while (data.containsKey(new ByteHolder(key))) {
          key = generateKey(random);
        }

        value = generateValue(random);
        data.put(new ByteHolder(key), new ByteHolder(value));

        digest.reset();
        sha1 = digest.digest(key);
        digest.reset();

        added = memTable.put(sha1, key, value);
        if (!added) {
          data.remove(new ByteHolder(key));
        }
      } while (added);

      assertEquals(156672, data.size());

      Set<ByteHolder> absentValues = generateNNotExistingEntries(data.size(), data, random);
      assertTable(data, absentValues, memTable, digest);

      long toHTableStart = System.nanoTime();
      final SerializedHTable serializedHTable = memTable.toHTable();
      long toHTableEnd = System.nanoTime();

      final OHTable hTable = new OHTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer(), 1);
      long assertHTableStart = System.nanoTime();
      assertTable(data, absentValues, hTable, digest);
      long assertHTableEnd = System.nanoTime();

      serializedHTable.free();
      assertHTableTime += (assertHTableEnd - assertHTableStart);
      convertHTableTime += (toHTableEnd - toHTableStart);
      counter++;
    }

    System.out.printf("htable conversion time : %d ns, htable assert time %d, ns\n", convertHTableTime / counter,
        assertHTableTime / counter);
  }

  private Set<ByteHolder> generateNNotExistingEntries(int n, Map<ByteHolder, ByteHolder> entries, Random random) {
    Set<ByteHolder> nonExisting = new HashSet<>();
    while (nonExisting.size() < n) {
      final byte[] key = generateKey(random);

      final ByteHolder holder = new ByteHolder(key);
      if (!entries.containsKey(holder)) {
        nonExisting.add(holder);
      }
    }

    return nonExisting;
  }

  private byte[] generateKey(Random random) {
    final int keySize = random.nextInt(17) + 8;
    final byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

  private Map<ByteHolder, ByteHolder> generateNEntries(int n, Random random) {
    final Map<ByteHolder, ByteHolder> entries = new HashMap<>();

    while (entries.size() < n) {
      final byte[] key = generateKey(random);

      final byte[] value = generateValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
    }

    return entries;
  }

  private byte[] generateValue(Random random) {
    final int valueSize = random.nextInt(30) + 15;
    final byte[] value = new byte[valueSize];
    random.nextBytes(value);
    return value;
  }

  private void fillMemTable(OMemTable table, Map<ByteHolder, ByteHolder> entries, MessageDigest digest) {
    for (Map.Entry<ByteHolder, ByteHolder> entry : entries.entrySet()) {
      digest.reset();
      final byte[] sha1 = digest.digest(entry.getKey().bytes);
      digest.reset();

      assertTrue(table.put(sha1, entry.getKey().bytes, entry.getValue().bytes));
    }
  }

  private void assertTable(Map<ByteHolder, ByteHolder> existingValues, Set<ByteHolder> absentValues, OTable table,
      MessageDigest digest) {
    for (Map.Entry<ByteHolder, ByteHolder> entry : existingValues.entrySet()) {
      digest.reset();
      final byte[] sha1 = digest.digest(entry.getKey().bytes);
      digest.reset();

      final byte[] missedKes = new byte[] { 123, -68, -90, 44, 41, 74, -91, 94, -80, -49, -3, -94, 4, 56, 6, 90, 105, 109, -9, -87,
          83, 25 };
      if (Arrays.equals(missedKes, entry.getKey().bytes)) {
        System.out.println();
      }
      assertArrayEquals(entry.getValue().bytes, table.get(entry.getKey().bytes, sha1));
    }

    for (ByteHolder key : absentValues) {
      digest.reset();
      final byte[] sha1 = digest.digest(key.bytes);
      digest.reset();

      assertNull(table.get(key.bytes, sha1));
    }
  }

  private class ByteHolder {
    private final byte[] bytes;

    private ByteHolder(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      ByteHolder that = (ByteHolder) o;
      return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
