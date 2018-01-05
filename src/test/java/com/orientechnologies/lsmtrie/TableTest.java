package com.orientechnologies.lsmtrie;

import com.google.common.util.concurrent.Striped;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

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
        MemTable memTable = new MemTable(1);

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

        final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer().asReadOnlyBuffer(),
            1, null, null);

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
      final MemTable memTable = new MemTable(1);

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

      final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer(), 1, null,
          null);
      long assertHTableStart = System.nanoTime();
      assertTable(data, absentValues, hTable, digest);
      long assertHTableEnd = System.nanoTime();

      serializedHTable.free();
      assertHTableTime += (assertHTableEnd - assertHTableStart);
      convertHTableTime += (toHTableEnd - toHTableStart);
      counter++;
    }

    System.out.printf("htable conversion time : %d ns, htable assert time %d ns, %d ns/item\n", convertHTableTime / counter,
        assertHTableTime / counter, assertHTableTime / counter / 156672);
  }

  @Test
  public void mtFillAndCheckTest() throws Exception {
    for (int n = 0; n < 10000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final ConcurrentHashMap<ByteHolder, ByteHolder> map = new ConcurrentHashMap<>();
      final MemTable memTable = new MemTable(1);

      final Striped<Lock> striped = Striped.lazyWeakLock(64);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new CheckedAdder(memTable, map, striped)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

      assertEquals(156_672, memTable.size());
      assertTrue(memTable.memorySize() <= 67_109_064);
      assertTrue(memTable.isFilled());
      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      assertTable(map, Collections.emptySet(), memTable, digest);
    }
  }

  @Test
  public void mtFullTest() throws Exception {
    for (int n = 0; n < 50000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final MemTable memTable = new MemTable(1);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new Adder(memTable)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

      assertEquals(156_672, memTable.size());
      assertTrue(memTable.memorySize() <= 67_109_064);
      assertTrue(memTable.isFilled());
    }
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

  private static byte[] generateKey(Random random) {
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

  private static byte[] generateValue(Random random) {
    final int valueSize = random.nextInt(30) + 15;
    final byte[] value = new byte[valueSize];
    random.nextBytes(value);
    return value;
  }

  private void fillMemTable(MemTable table, Map<ByteHolder, ByteHolder> entries, MessageDigest digest) {
    for (Map.Entry<ByteHolder, ByteHolder> entry : entries.entrySet()) {
      digest.reset();
      final byte[] sha1 = digest.digest(entry.getKey().bytes);
      digest.reset();

      assertTrue(table.put(sha1, entry.getKey().bytes, entry.getValue().bytes));
    }
  }

  private void assertTable(Map<ByteHolder, ByteHolder> existingValues, Set<ByteHolder> absentValues, Table table,
      MessageDigest digest) {
    for (Map.Entry<ByteHolder, ByteHolder> entry : existingValues.entrySet()) {
      digest.reset();
      final byte[] sha1 = digest.digest(entry.getKey().bytes);
      digest.reset();

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

  private final class CheckedAdder implements Callable<Void> {
    private final MemTable                                  memTable;
    private final ConcurrentHashMap<ByteHolder, ByteHolder> map;
    private final Random random = new Random();
    private final MessageDigest messageDigest;
    private final Striped<Lock> striped;

    private CheckedAdder(MemTable memTable, ConcurrentHashMap<ByteHolder, ByteHolder> map, Striped<Lock> striped) throws Exception {
      this.memTable = memTable;
      this.map = map;
      this.striped = striped;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() throws Exception {
      while (true) {
        final byte[] key = generateKey(random);
        final byte[] value = generateValue(random);

        messageDigest.reset();

        ByteHolder keyHolder = new ByteHolder(key);
        ByteHolder valueHolder = new ByteHolder(value);

        final Lock lock = striped.get(key);
        lock.lock();
        try {
          byte[] sha1 = messageDigest.digest(key);
          final boolean put = memTable.put(sha1, key, value);
          if (put) {
            map.put(keyHolder, valueHolder);
          } else {
            break;
          }
        } finally {
          lock.unlock();
        }
      }

      return null;
    }
  }

  private final class Adder implements Callable<Void> {
    private final MemTable memTable;
    private final Random random = new Random();
    private final MessageDigest messageDigest;

    private Adder(MemTable memTable) throws Exception {
      this.memTable = memTable;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() {
      while (true) {
        final byte[] key = generateKey(random);
        final byte[] value = generateValue(random);

        messageDigest.reset();

        final byte[] sha1 = messageDigest.digest(key);
        final boolean put = memTable.put(sha1, key, value);
        if (!put) {
          break;
        }
      }

      return null;
    }
  }
}
