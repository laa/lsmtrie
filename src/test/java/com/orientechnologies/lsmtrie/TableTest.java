package com.orientechnologies.lsmtrie;

import com.google.common.util.concurrent.Striped;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        assertHTableIterator(entries, hTable, digest);

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
  public void testAddNBigValues() throws Exception {
    for (int k = 0; k < 600; k++) {
      for (int i = 0; i < 17; i++) {
        final int items = 1 << i;

        final long seed = System.nanoTime();

        System.out.println("testAdd " + items + " big items seed : " + seed);
        final Random random = new Random(seed);

        Map<ByteHolder, ByteHolder> entries = generateNBigEntries(items, random);
        assertEquals(items, entries.size());
        MemTable memTable = new MemTable(1);

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        fillMemTable(memTable, entries, digest);
        assertTrue(memTable.memorySize() < Table.TOTAL_SIZE);

        Set<ByteHolder> nonExisting = generateNNotExistingEntries(items, entries, random);
        assertEquals(items, nonExisting.size());

        assertTable(entries, nonExisting, memTable, digest);

        SerializedHTable serializedHTable = memTable.toHTable();

        final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer().asReadOnlyBuffer(),
            1, null, null);

        assertTable(entries, nonExisting, hTable, digest);
        assertHTableIterator(entries, hTable, digest);

        serializedHTable.free();
      }
    }
  }

  @Test
  public void testAddNMixedValues() throws Exception {
    for (int k = 0; k < 600; k++) {
      for (int i = 0; i < 17; i++) {
        final int items = 1 << i;

        final long seed = System.nanoTime();

        System.out.println("testAdd " + items + " mixed items seed : " + seed);
        final Random random = new Random(seed);

        Map<ByteHolder, ByteHolder> entries = generateNMixedEntries(items, random);
        assertEquals(items, entries.size());
        MemTable memTable = new MemTable(1);

        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        fillMemTable(memTable, entries, digest);
        assertTrue(memTable.memorySize() < Table.TOTAL_SIZE);

        Set<ByteHolder> nonExisting = generateNNotExistingEntries(items, entries, random);
        assertEquals(items, nonExisting.size());

        assertTable(entries, nonExisting, memTable, digest);

        SerializedHTable serializedHTable = memTable.toHTable();

        final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer().asReadOnlyBuffer(),
            1, null, null);

        assertTable(entries, nonExisting, hTable, digest);
        assertHTableIterator(entries, hTable, digest);

        serializedHTable.free();
      }
    }
  }

  @Test
  public void testTillFull() throws Exception {
    long assertHTableTime = 0;
    long convertHTableTime = 0;
    int counter = 0;
    long totalSize = 0;

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

      totalSize += data.size();

      Set<ByteHolder> absentValues = generateNNotExistingEntries(data.size(), data, random);
      assertTable(data, absentValues, memTable, digest);

      long toHTableStart = System.nanoTime();
      final SerializedHTable serializedHTable = memTable.toHTable();
      long toHTableEnd = System.nanoTime();

      final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer(), 1, null, null);
      long assertHTableStart = System.nanoTime();
      assertTable(data, absentValues, hTable, digest);
      long assertHTableEnd = System.nanoTime();
      assertHTableIterator(data, hTable, digest);

      serializedHTable.free();
      assertHTableTime += (assertHTableEnd - assertHTableStart);
      convertHTableTime += (toHTableEnd - toHTableStart);
      counter++;
    }

    System.out.printf("htable conversion time : %d ns, htable assert time %d ns, %d ns/item\n", convertHTableTime / counter,
        assertHTableTime / counter, assertHTableTime / totalSize);
  }

  @Test
  public void testTillFullBigValues() throws Exception {
    for (int k = 0; k < 3600; k++) {
      final Map<ByteHolder, ByteHolder> data = new HashMap<>();
      final MemTable memTable = new MemTable(1);

      final long seed = System.nanoTime();
      System.out.println("testTillFullBig seed : " + seed);
      Random random = new Random(seed);

      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      byte[] key;
      byte[] value;
      byte[] sha1;
      boolean added;
      do {
        key = generateBigKey(random);

        while (data.containsKey(new ByteHolder(key))) {
          key = generateBigKey(random);
        }

        value = generateBigValue(random);
        data.put(new ByteHolder(key), new ByteHolder(value));

        digest.reset();
        sha1 = digest.digest(key);
        digest.reset();

        added = memTable.put(sha1, key, value);
        if (!added) {
          data.remove(new ByteHolder(key));
        }
      } while (added);

      Set<ByteHolder> absentValues = generateNNotExistingEntries(data.size(), data, random);
      assertTable(data, absentValues, memTable, digest);

      final SerializedHTable serializedHTable = memTable.toHTable();

      final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer(), 1, null, null);
      assertTable(data, absentValues, hTable, digest);
      assertHTableIterator(data, hTable, digest);

      serializedHTable.free();
    }
  }

  @Test
  public void testTillFullMixedValues() throws Exception {
    for (int k = 0; k < 3600; k++) {
      final Map<ByteHolder, ByteHolder> data = new HashMap<>();
      final MemTable memTable = new MemTable(1);

      final long seed = System.nanoTime();
      System.out.println("testTillFullMixed seed : " + seed);
      Random random = new Random(seed);

      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      byte[] key;
      byte[] value;
      byte[] sha1;
      boolean added;
      do {
        key = generateMixedKey(random);

        while (data.containsKey(new ByteHolder(key))) {
          key = generateMixedKey(random);
        }

        value = generateMixedValue(random);
        data.put(new ByteHolder(key), new ByteHolder(value));

        digest.reset();
        sha1 = digest.digest(key);
        digest.reset();

        added = memTable.put(sha1, key, value);
        if (!added) {
          data.remove(new ByteHolder(key));
        }
      } while (added);

      Set<ByteHolder> absentValues = generateNNotExistingEntries(data.size(), data, random);
      assertTable(data, absentValues, memTable, digest);

      final SerializedHTable serializedHTable = memTable.toHTable();

      final HTable hTable = new HTable(serializedHTable.getBloomFilters(), serializedHTable.getHtableBuffer(), 1, null, null);
      assertTable(data, absentValues, hTable, digest);
      assertHTableIterator(data, hTable, digest);

      serializedHTable.free();
    }
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

      assertTrue(memTable.isFilled());
      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      assertTable(map, Collections.emptySet(), memTable, digest);
    }
  }

  @Test
  public void mtFillAndCheckBigTest() throws Exception {
    for (int n = 0; n < 10000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final ConcurrentHashMap<ByteHolder, ByteHolder> map = new ConcurrentHashMap<>();
      final MemTable memTable = new MemTable(1);

      final Striped<Lock> striped = Striped.lazyWeakLock(64);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new CheckedBigAdder(memTable, map, striped)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

      assertTrue(memTable.isFilled());
      MessageDigest digest = MessageDigest.getInstance("SHA-1");
      assertTable(map, Collections.emptySet(), memTable, digest);
    }
  }

  @Test
  public void mtFillAndCheckMixedTest() throws Exception {
    for (int n = 0; n < 10000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final ConcurrentHashMap<ByteHolder, ByteHolder> map = new ConcurrentHashMap<>();
      final MemTable memTable = new MemTable(1);

      final Striped<Lock> striped = Striped.lazyWeakLock(64);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new CheckedMixedAdder(memTable, map, striped)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

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

      assertTrue(memTable.isFilled());
    }
  }

  @Test
  public void mtFullBigTest() throws Exception {
    for (int n = 0; n < 50000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final MemTable memTable = new MemTable(1);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new BigAdder(memTable)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

      assertTrue(memTable.isFilled());
    }
  }

  @Test
  public void mtFullMixedTest() throws Exception {
    for (int n = 0; n < 50000; n++) {
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final MemTable memTable = new MemTable(1);
      final List<Future<Void>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new MixedAdder(memTable)));
      }

      for (Future<Void> future : futures) {
        future.get();
      }

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

  private static byte[] generateBigKey(Random random) {
    final int keySize = random.nextInt(128) + 256;
    final byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

  private static byte[] generateMixedKey(Random random) {
    final int keySize = random.nextInt(256) + 8;
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

  private Map<ByteHolder, ByteHolder> generateNBigEntries(int n, Random random) {
    final Map<ByteHolder, ByteHolder> entries = new HashMap<>();

    while (entries.size() < n) {
      final byte[] key = generateBigKey(random);

      final byte[] value = generateBigValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
    }

    return entries;
  }

  private Map<ByteHolder, ByteHolder> generateNMixedEntries(int n, Random random) {
    final Map<ByteHolder, ByteHolder> entries = new HashMap<>();

    while (entries.size() < n) {
      final byte[] key = generateMixedKey(random);

      final byte[] value = generateMixedValue(random);

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

  private static byte[] generateBigValue(Random random) {
    final int valueSize = random.nextInt(125) + 256;
    final byte[] value = new byte[valueSize];
    random.nextBytes(value);
    return value;
  }

  private static byte[] generateMixedValue(Random random) {
    final int valueSize = random.nextInt(256) + 8;
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

  private void assertHTableIterator(Map<ByteHolder, ByteHolder> existingValues, HTable table, MessageDigest digest) {
    int counter = 0;

    for (int i = 0; i < Table.BUCKETS_COUNT; i++) {
      final Iterator<byte[][]> bucketIterator = table.bucketIterator(i);

      while (bucketIterator.hasNext()) {
        final byte[][] entry = bucketIterator.next();
        assertArrayEquals(existingValues.get(new ByteHolder(entry[1])).bytes, entry[2]);
        counter++;

        if (entry[0] != null) {
          digest.reset();

          final byte[] sha1 = digest.digest(entry[1]);
          assertArrayEquals(sha1, entry[0]);
        }
      }
    }

    assertEquals(existingValues.size(), counter);
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
    public Void call() {
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

  private final class CheckedBigAdder implements Callable<Void> {
    private final MemTable                                  memTable;
    private final ConcurrentHashMap<ByteHolder, ByteHolder> map;
    private final Random random = new Random();
    private final MessageDigest messageDigest;
    private final Striped<Lock> striped;

    private CheckedBigAdder(MemTable memTable, ConcurrentHashMap<ByteHolder, ByteHolder> map, Striped<Lock> striped) throws Exception {
      this.memTable = memTable;
      this.map = map;
      this.striped = striped;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() {
      while (true) {
        final byte[] key = generateBigKey(random);
        final byte[] value = generateBigValue(random);

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


  private final class CheckedMixedAdder implements Callable<Void> {
    private final MemTable                                  memTable;
    private final ConcurrentHashMap<ByteHolder, ByteHolder> map;
    private final Random random = new Random();
    private final MessageDigest messageDigest;
    private final Striped<Lock> striped;

    private CheckedMixedAdder(MemTable memTable, ConcurrentHashMap<ByteHolder, ByteHolder> map, Striped<Lock> striped) throws Exception {
      this.memTable = memTable;
      this.map = map;
      this.striped = striped;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() {
      while (true) {
        final byte[] key = generateMixedKey(random);
        final byte[] value = generateMixedValue(random);

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

  private final class BigAdder implements Callable<Void> {
    private final MemTable memTable;
    private final Random random = new Random();
    private final MessageDigest messageDigest;

    private BigAdder(MemTable memTable) throws Exception {
      this.memTable = memTable;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() {
      while (true) {
        final byte[] key = generateBigKey(random);
        final byte[] value = generateBigValue(random);

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

  private final class MixedAdder implements Callable<Void> {
    private final MemTable memTable;
    private final Random random = new Random();
    private final MessageDigest messageDigest;

    private MixedAdder(MemTable memTable) throws Exception {
      this.memTable = memTable;
      messageDigest = MessageDigest.getInstance("SHA-1");
    }

    @Override
    public Void call() {
      while (true) {
        final byte[] key = generateMixedKey(random);
        final byte[] value = generateMixedValue(random);

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
