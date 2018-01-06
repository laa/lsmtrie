package com.orientechnologies.lsmtrie;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LSMTrieTest {
  private static Path buildDirectory;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = Paths.get(System.getProperty("buildDirectory", "target")).resolve("LSMTrieTest");
  }

  @Before
  public void beforeMethod() throws Exception {
    removeRecursively(buildDirectory);
  }

  @Test
  public void testAddNkeys() throws Exception {
    int n = 2 * 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddNkeys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNEntries(data, n, random);
    System.out.println("Entries generation completed");

    long fillStart = System.nanoTime();
    for (Map.Entry<ByteHolder, ByteHolder> entry : data.entrySet()) {
      lsmTrie.put(entry.getKey().bytes, entry.getValue().bytes);
    }
    long fillEnd = System.nanoTime();

    System.out.printf("Load speed for %d, items is :%d ns/item, %d op/s \n", n, (fillEnd - fillStart) / n,
        n * 1000_000_000L / (fillEnd - fillStart));

    Set<ByteHolder> nonExistingData = generateNNotExistingEntries(10000, data, random);

    for (int k = 0; k < 5; k++) {
      System.out.printf("%d check \n", k + 1);
      long assertStart = System.nanoTime();
      assertTable(data, nonExistingData, lsmTrie);
      long assertEnd = System.nanoTime();
      System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / n,
          n * 1000_000_000L / (assertEnd - assertStart));
    }

    System.out.println("Close trie");
    lsmTrie.close();

    lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    System.out.println("Load trie");
    lsmTrie.load();

    System.out.println("Assertion check");
    long assertStart = System.nanoTime();
    assertTable(data, nonExistingData, lsmTrie);
    long assertEnd = System.nanoTime();
    System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / n,
        n * 1000_000_000L / (assertEnd - assertStart));

    lsmTrie.delete();
  }

  @Test
  public void testAddNNkeysOpenClose() throws Exception {
    int n = 2 * 8 * 8 * 156_672;
    final long seed = System.nanoTime();
    System.out.println("testAddNkeys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    for (int i = 0; i < 10; i++) {
      OLSMTrie lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
      lsmTrie.load();

      Map<ByteHolder, ByteHolder> data = new HashMap<>();
      System.out.println("Entries generation started");
      generateNEntries(data, n, random);
      System.out.println("Entries generation completed");

      long fillStart = System.nanoTime();
      for (Map.Entry<ByteHolder, ByteHolder> entry : data.entrySet()) {
        lsmTrie.put(entry.getKey().bytes, entry.getValue().bytes);
      }
      long fillEnd = System.nanoTime();

      System.out.printf("Load speed for %d, items is :%d ns/item, %d op/s \n", n, (fillEnd - fillStart) / n,
          n * 1000_000_000L / (fillEnd - fillStart));

      Set<ByteHolder> nonExistingData = generateNNotExistingEntries(10000, data, random);

      for (int k = 0; k < 5; k++) {
        System.out.printf("%d check \n", k + 1);
        long assertStart = System.nanoTime();
        assertTable(data, nonExistingData, lsmTrie);
        long assertEnd = System.nanoTime();
        System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / n,
            n * 1000_000_000L / (assertEnd - assertStart));
      }

      System.out.println("Close trie");
      lsmTrie.close();
    }

    OLSMTrie lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    lsmTrie.load();
    lsmTrie.delete();
  }

  @Test
  public void testAddDuplicates() throws Exception {
    int n = 1_000_000;

    final long seed = System.nanoTime();
    System.out.println("testAddDuplicates seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddDuplicates", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNEntries(data, n, random);
    System.out.println("Entries generation completed");
    List<Map.Entry<ByteHolder, ByteHolder>> entries = new ArrayList<>(data.entrySet());
    Map<ByteHolder, ByteHolder> addedData = new HashMap<>();

    for (int k = 0; k < 50; k++) {
      System.out.printf("Add %d millions\n", k);
      for (int i = 0; i < 1_000_000; i++) {
        final int index = random.nextInt(entries.size());
        final Map.Entry<ByteHolder, ByteHolder> entry = entries.get(index);
        lsmTrie.put(entry.getKey().bytes, entry.getValue().bytes);
        addedData.put(entry.getKey(), entry.getValue());
      }

      System.out.printf("%d assertion \n", k);
      assertTable(addedData, Collections.emptySet(), lsmTrie);
    }

    System.out.println("Closing LSM trie");
    lsmTrie.close();

    System.out.println("Loading LSM trie");
    lsmTrie.load();

    System.out.println("Asserting LSM trie");
    assertTable(addedData, Collections.emptySet(), lsmTrie);
    lsmTrie.delete();
  }

  @Test
  public void testAddN2keys() throws Exception {
    int n = 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddN2keys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNEntries(data, n, random);
    System.out.println("Entries generation completed");

    long fillStart = System.nanoTime();
    for (Map.Entry<ByteHolder, ByteHolder> entry : data.entrySet()) {
      lsmTrie.put(entry.getKey().bytes, entry.getValue().bytes);
    }
    long fillEnd = System.nanoTime();

    System.out.printf("Load speed for %d, items is :%d ns/item, %d op/s \n", n, (fillEnd - fillStart) / n,
        n * 1000_000_000L / (fillEnd - fillStart));

    Set<ByteHolder> nonExistingData = generateNNotExistingEntries(10000, data, random);

    for (int k = 0; k < 5; k++) {
      System.out.printf("%d check \n", k + 1);
      long assertStart = System.nanoTime();
      assertTable(data, nonExistingData, lsmTrie);
      long assertEnd = System.nanoTime();
      System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / n,
          n * 1000_000_000L / (assertEnd - assertStart));
    }

    System.out.println("Close trie");
    lsmTrie.close();

    lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    System.out.println("Load trie");
    lsmTrie.load();

    System.out.println("Assertion check");
    long assertStart = System.nanoTime();
    assertTable(data, nonExistingData, lsmTrie);
    long assertEnd = System.nanoTime();
    System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / n,
        n * 1000_000_000L / (assertEnd - assertStart));

    System.out.println("Entries generation started");
    generateNEntries(data, n, random);
    System.out.println("Entries generation completed");

    fillStart = System.nanoTime();
    for (Map.Entry<ByteHolder, ByteHolder> entry : data.entrySet()) {
      lsmTrie.put(entry.getKey().bytes, entry.getValue().bytes);
    }
    fillEnd = System.nanoTime();

    System.out.printf("Load speed for %d, items is :%d ns/item, %d op/s \n", n, (fillEnd - fillStart) / n,
        n * 1000_000_000L / (fillEnd - fillStart));

    for (int k = 0; k < 5; k++) {
      System.out.printf("%d check \n", k + 1);
      assertStart = System.nanoTime();
      assertTable(data, nonExistingData, lsmTrie);
      assertEnd = System.nanoTime();
      System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / (2 * n),
          2 * n * 1000_000_000L / (assertEnd - assertStart));
    }

    System.out.println("Close trie");
    lsmTrie.close();

    lsmTrie = new OLSMTrie("testAddNkeys", buildDirectory);
    System.out.println("Load trie");
    lsmTrie.load();

    System.out.println("Assertion check");
    assertStart = System.nanoTime();
    assertTable(data, nonExistingData, lsmTrie);
    assertEnd = System.nanoTime();
    System.out.printf("Assertion speed is %d ns/item, %d op/s \n", (assertEnd - assertStart) / (2 * n),
        2 * n * 1000_000_000L / (assertEnd - assertStart));

    lsmTrie.delete();
  }

  private static byte[] generateKey(Random random) {
    final int keySize = random.nextInt(17) + 8;
    final byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

  private Map<ByteHolder, ByteHolder> generateNEntries(Map<ByteHolder, ByteHolder> entries, int n, Random random) {
    while (entries.size() < n) {
      final byte[] key = generateKey(random);

      final byte[] value = generateValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
    }

    return entries;
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

  private static byte[] generateValue(Random random) {
    final int valueSize = random.nextInt(30) + 15;
    final byte[] value = new byte[valueSize];
    random.nextBytes(value);
    return value;
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

  private void assertTable(Map<ByteHolder, ByteHolder> existingValues, Set<ByteHolder> absentValues, OLSMTrie table) {
    for (Map.Entry<ByteHolder, ByteHolder> entry : existingValues.entrySet()) {
      assertArrayEquals(entry.getValue().bytes, table.get(entry.getKey().bytes));
    }

    for (ByteHolder key : absentValues) {
      assertNull(table.get(key.bytes));
    }
  }

  private void removeRecursively(Path path) throws IOException {
    if (Files.exists(path)) {
      Files.list(path).forEach(p -> {
        if (Files.isDirectory(p)) {
          try {
            removeRecursively(p);
          } catch (IOException e) {
            throw new IllegalStateException("Can not delete file", e);
          }

        } else {
          try {
            Files.delete(p);
          } catch (IOException e) {
            throw new IllegalStateException("Can not delete file", e);
          }
        }
      });
    }

  }
}
