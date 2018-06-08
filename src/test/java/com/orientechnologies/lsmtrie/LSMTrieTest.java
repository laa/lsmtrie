package com.orientechnologies.lsmtrie;

import com.google.common.util.concurrent.Striped;
import org.junit.Assert;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

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
  public void testAddNBigkeys() throws Exception {
    int n = 2 * 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddNBigkeys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddNBigkeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNBigEntries(data, n, random);
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
  public void testAddNMixedKeys() throws Exception {
    int n = 2 * 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddNMixed keys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddNMixedKeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNMixedEntries(data, n, random);
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
  public void testAddNNBigkeysOpenClose() throws Exception {
    int n = 2 * 8 * 8 * 156_672;
    final long seed = System.nanoTime();
    System.out.println("testAddNBigKeysOpenClose (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    for (int i = 0; i < 10; i++) {
      OLSMTrie lsmTrie = new OLSMTrie("testAddNBigKeysOpenClose", buildDirectory);
      lsmTrie.load();

      Map<ByteHolder, ByteHolder> data = new HashMap<>();
      System.out.println("Entries generation started");
      generateNBigEntries(data, n, random);
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

    OLSMTrie lsmTrie = new OLSMTrie("testAddNBigKeysOpenClose", buildDirectory);
    lsmTrie.load();
    lsmTrie.delete();
  }

  @Test
  public void testAddNNMixedKeysOpenClose() throws Exception {
    int n = 2 * 8 * 8 * 156_672;
    final long seed = System.nanoTime();
    System.out.println("testAddNNMixedKeysOpenClose (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    for (int i = 0; i < 10; i++) {
      OLSMTrie lsmTrie = new OLSMTrie("testAddNNMixedKeysOpenClose", buildDirectory);
      lsmTrie.load();

      Map<ByteHolder, ByteHolder> data = new HashMap<>();
      System.out.println("Entries generation started");
      generateNMixedEntries(data, n, random);
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

    OLSMTrie lsmTrie = new OLSMTrie("testAddNNMixedKeysOpenClose", buildDirectory);
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
  public void testAddBigDuplicates() throws Exception {
    int n = 1_000_000;

    final long seed = System.nanoTime();
    System.out.println("testAddBigDuplicates seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddBigDuplicates", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNBigEntries(data, n, random);
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
  public void testAddMixedDuplicates() throws Exception {
    int n = 1_000_000;

    final long seed = System.nanoTime();
    System.out.println("testAddMixedDuplicates seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddMixedDuplicates", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNMixedEntries(data, n, random);
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

  @Test
  public void testAddN2Mixedkeys() throws Exception {
    int n = 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddN2Mixedkeys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddN2Mixedkeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNMixedEntries(data, n, random);
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

    lsmTrie = new OLSMTrie("testAddN2Mixedkeys", buildDirectory);
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

    lsmTrie = new OLSMTrie("testAddN2Mixedkeys", buildDirectory);
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

  @Test
  public void testAddN2BigKeys() throws Exception {
    int n = 8 * 8 * 156_672;

    final long seed = System.nanoTime();
    System.out.println("testAddN2BigKeys (" + n + " keys) seed: " + seed);

    final Random random = new Random(seed);
    OLSMTrie lsmTrie = new OLSMTrie("testAddN2BigKeys", buildDirectory);
    lsmTrie.load();

    Map<ByteHolder, ByteHolder> data = new HashMap<>();
    System.out.println("Entries generation started");
    generateNBigEntries(data, n, random);
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

    lsmTrie = new OLSMTrie("testAddN2BigKeys", buildDirectory);
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

    lsmTrie = new OLSMTrie("testAddN2BigKeys", buildDirectory);
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

  @Test
  public void mtTestOnlyFill() throws Exception {
    for (int k = 0; k < 10; k++) {
      int n = 2 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFill", buildDirectory);
      lsmTrie.load();

      final ConcurrentHashMap<ByteHolder, ByteHolder> data = new ConcurrentHashMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new Writer(n, data, striped, lsmTrie)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.printf("Writers are done %d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      System.out.println("Load table");
      lsmTrie = new OLSMTrie("mtTestOnlyFill", buildDirectory);
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyFillBigKey() throws Exception {
    for (int k = 0; k < 10; k++) {
      int n = 2 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFillBigKeys", buildDirectory);
      lsmTrie.load();

      final ConcurrentHashMap<ByteHolder, ByteHolder> data = new ConcurrentHashMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new WriterBigKeys(n, data, striped, lsmTrie)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.printf("Writers are done %d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      System.out.println("Load table");
      lsmTrie = new OLSMTrie("mtTestOnlyFillBigKeys", buildDirectory);
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyFillMixedKeys() throws Exception {
    for (int k = 0; k < 10; k++) {
      int n = 2 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> futures = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFillMixedKeys", buildDirectory);
      lsmTrie.load();

      final ConcurrentHashMap<ByteHolder, ByteHolder> data = new ConcurrentHashMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new WriterMixedKeys(n, data, striped, lsmTrie)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : futures) {
        future.get();
      }

      System.out.printf("Writers are done %d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      System.out.println("Load table");
      lsmTrie = new OLSMTrie("mtTestOnlyFillMixedKeys", buildDirectory);
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyHafFillHalfRead() throws Exception {
    for (int k = 0; k < 1; k++) {
      int n = 4 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> writers = new ArrayList<>();
      final List<Future<Void>> readers = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfRead", buildDirectory);
      lsmTrie.load();

      final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 4; i++) {
        writers.add(executorService.submit(new Writer(n, data, striped, lsmTrie)));
      }

      final AtomicBoolean stop = new AtomicBoolean();
      System.out.println("Starting readers");
      for (int i = 0; i < 4; i++) {
        readers.add(executorService.submit(new Reader(data, striped, lsmTrie, stop)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : writers) {
        future.get();
      }

      stop.set(true);

      System.out.println("Waiting till readers completion");
      for (Future<Void> future : readers) {
        future.get();
      }

      System.out.printf("%d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfRead", buildDirectory);
      System.out.println("Load table");
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyHafFillHalfReadBigKeys() throws Exception {
    for (int k = 0; k < 10; k++) {
      int n = 4 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> writers = new ArrayList<>();
      final List<Future<Void>> readers = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfReadBigKeys", buildDirectory);
      lsmTrie.load();

      final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 4; i++) {
        writers.add(executorService.submit(new WriterBigKeys(n, data, striped, lsmTrie)));
      }

      final AtomicBoolean stop = new AtomicBoolean();
      System.out.println("Starting readers");
      for (int i = 0; i < 4; i++) {
        readers.add(executorService.submit(new ReaderBigKeys(data, striped, lsmTrie, stop)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : writers) {
        future.get();
      }

      stop.set(true);

      System.out.println("Waiting till readers completion");
      for (Future<Void> future : readers) {
        future.get();
      }

      System.out.printf("%d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfReadBigKeys", buildDirectory);
      System.out.println("Load table");
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyHafFillHalfReadMixedKeys() throws Exception {
    for (int k = 0; k < 10; k++) {
      int n = 4 * 8 * 156_672;
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final List<Future<Void>> writers = new ArrayList<>();
      final List<Future<Void>> readers = new ArrayList<>();

      OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfReadMixedKeys", buildDirectory);
      lsmTrie.load();

      final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
      final Striped<Lock> striped = Striped.lazyWeakLock(1024);

      System.out.println("Starting writers");
      for (int i = 0; i < 4; i++) {
        writers.add(executorService.submit(new WriterMixedKeys(n, data, striped, lsmTrie)));
      }

      final AtomicBoolean stop = new AtomicBoolean();
      System.out.println("Starting readers");
      for (int i = 0; i < 4; i++) {
        readers.add(executorService.submit(new ReaderMixedKeys(data, striped, lsmTrie, stop)));
      }

      System.out.println("Waiting till writers completion");
      for (Future<Void> future : writers) {
        future.get();
      }

      stop.set(true);

      System.out.println("Waiting till readers completion");
      for (Future<Void> future : readers) {
        future.get();
      }

      System.out.printf("%d items were added, assert table\n", data.size());
      for (int i = 0; i < 2; i++) {
        System.out.printf("%d assert \n", i);
        assertTable(data, Collections.emptySet(), lsmTrie);
      }

      System.out.println("Close table");
      lsmTrie.close();

      lsmTrie = new OLSMTrie("mtTestOnlyHafFillHalfReadMixedKeys", buildDirectory);
      System.out.println("Load table");
      lsmTrie.load();

      System.out.println("Assert table");
      assertTable(data, Collections.emptySet(), lsmTrie);
      lsmTrie.delete();
    }
  }

  @Test
  public void mtTestOnlyFillAndUpdate() throws Exception {
    OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdate", buildDirectory);
    lsmTrie.load();

    final ExecutorService executorService = Executors.newCachedThreadPool();
    final List<Future<Void>> futures = new ArrayList<>();

    final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
    final Striped<Lock> striped = Striped.lazyWeakLock(1024);

    final AtomicBoolean stop = new AtomicBoolean();

    final AtomicInteger sizeCounter = new AtomicInteger();
    System.out.println("Start data handling threads");
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new Modifier(500_000, 20_000_000, sizeCounter, data, striped, lsmTrie, stop)));
    }

    final int sec = 60 * 60;

    System.out.printf("Wait during %d second\n", sec);
    Thread.sleep(sec * 1000);

    stop.set(true);

    System.out.println("Wait for data handling threads to stop");
    for (Future<Void> future : futures) {
      future.get();
    }

    System.out.printf("%d items were added, assert table\n", data.size());
    for (int i = 0; i < 5; i++) {
      System.out.printf("%d assert \n", i);
      assertTable(data, Collections.emptySet(), lsmTrie);
    }

    System.out.println("Close table");
    lsmTrie.close();

    System.out.println("Load table");
    lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdate", buildDirectory);
    lsmTrie.load();

    System.out.println("Assert table");
    assertTable(data, Collections.emptySet(), lsmTrie);
    lsmTrie.delete();
  }

  @Test
  public void mtTestOnlyFillAndUpdateBigKeys() throws Exception {
    OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdateBigKeys", buildDirectory);
    lsmTrie.load();

    final ExecutorService executorService = Executors.newCachedThreadPool();
    final List<Future<Void>> futures = new ArrayList<>();

    final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
    final Striped<Lock> striped = Striped.lazyWeakLock(1024);

    final AtomicBoolean stop = new AtomicBoolean();

    final AtomicInteger sizeCounter = new AtomicInteger();
    System.out.println("Start data handling threads");
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new ModifierBigKeys(500_000, 20_000_000, sizeCounter, data, striped, lsmTrie, stop)));
    }

    final int sec = 60 * 60;

    System.out.printf("Wait during %d second\n", sec);
    Thread.sleep(sec * 1000);

    stop.set(true);

    System.out.println("Wait for data handling threads to stop");
    for (Future<Void> future : futures) {
      future.get();
    }

    System.out.printf("%d items were added, assert table\n", data.size());
    for (int i = 0; i < 5; i++) {
      System.out.printf("%d assert \n", i);
      assertTable(data, Collections.emptySet(), lsmTrie);
    }

    System.out.println("Close table");
    lsmTrie.close();

    System.out.println("Load table");
    lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdateBigKeys", buildDirectory);
    lsmTrie.load();

    System.out.println("Assert table");
    assertTable(data, Collections.emptySet(), lsmTrie);
    lsmTrie.delete();
  }

  @Test
  public void mtTestOnlyFillAndUpdateMixedKeys() throws Exception {
    OLSMTrie lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdateMixedKeys", buildDirectory);
    lsmTrie.load();

    final ExecutorService executorService = Executors.newCachedThreadPool();
    final List<Future<Void>> futures = new ArrayList<>();

    final ConcurrentSkipListMap<ByteHolder, ByteHolder> data = new ConcurrentSkipListMap<>();
    final Striped<Lock> striped = Striped.lazyWeakLock(1024);

    final AtomicBoolean stop = new AtomicBoolean();

    final AtomicInteger sizeCounter = new AtomicInteger();
    System.out.println("Start data handling threads");
    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new ModifierMixedKeys(500_000, 20_000_000, sizeCounter, data, striped, lsmTrie, stop)));
    }

    final int sec = 60 * 60;

    System.out.printf("Wait during %d second\n", sec);
    Thread.sleep(sec * 1000);

    stop.set(true);

    System.out.println("Wait for data handling threads to stop");
    for (Future<Void> future : futures) {
      future.get();
    }

    System.out.printf("%d items were added, assert table\n", data.size());
    for (int i = 0; i < 5; i++) {
      System.out.printf("%d assert \n", i);
      assertTable(data, Collections.emptySet(), lsmTrie);
    }

    System.out.println("Close table");
    lsmTrie.close();

    System.out.println("Load table");
    lsmTrie = new OLSMTrie("mtTestOnlyFillAndUpdateMixedKeys", buildDirectory);
    lsmTrie.load();

    System.out.println("Assert table");
    assertTable(data, Collections.emptySet(), lsmTrie);
    lsmTrie.delete();
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

  private void generateNEntries(Map<ByteHolder, ByteHolder> entries, int n, Random random) {
    while (entries.size() < n) {
      final byte[] key = generateKey(random);

      final byte[] value = generateValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
    }
  }

  private void generateNBigEntries(Map<ByteHolder, ByteHolder> entries, int n, Random random) {
    while (entries.size() < n) {
      final byte[] key = generateBigKey(random);

      final byte[] value = generateBigValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
    }
  }

  private void generateNMixedEntries(Map<ByteHolder, ByteHolder> entries, int n, Random random) {
    while (entries.size() < n) {
      final byte[] key = generateMixedKey(random);

      final byte[] value = generateMixedValue(random);

      entries.put(new ByteHolder(key), new ByteHolder(value));
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

  private static class ByteHolder implements Comparable<ByteHolder> {
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

    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") ByteHolder other) {

      if (other.bytes.length == bytes.length) {
        for (int i = 0; i < bytes.length; i++) {
          final int cmp = Byte.compare(bytes[i], other.bytes[i]);

          if (cmp != 0) {
            return cmp;
          }
        }
      } else if (bytes.length > other.bytes.length) {
        return 1;
      } else {
        return -1;
      }

      return 0;
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

  private final static class Writer implements Callable<Void> {
    private final int                                   countOfItemsToAdd;
    private final ConcurrentMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                         striped;
    private final OLSMTrie                              lsmTrie;
    private final ThreadLocalRandom                     random = ThreadLocalRandom.current();

    Writer(int countOfItemsToAdd, ConcurrentMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie) {
      this.countOfItemsToAdd = countOfItemsToAdd;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
    }

    @Override
    public Void call() {
      try {
        for (int i = 0; i < countOfItemsToAdd; i++) {
          final byte[] key = generateKey(random);
          final byte[] value = generateValue(random);

          final ByteHolder holderKey = new ByteHolder(key);
          final Lock lock = striped.get(holderKey);
          lock.lock();
          try {
            data.put(holderKey, new ByteHolder(value));
            lsmTrie.put(key, value);
          } finally {
            lock.unlock();
          }

          if (i > 0 & i % 100_000 == 0) {
            System.out.printf("%d writer thread, %d items processed\n", Thread.currentThread().getId(), i);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }

  private final static class WriterBigKeys implements Callable<Void> {
    private final int                                   countOfItemsToAdd;
    private final ConcurrentMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                         striped;
    private final OLSMTrie                              lsmTrie;
    private final ThreadLocalRandom                     random = ThreadLocalRandom.current();

    WriterBigKeys(int countOfItemsToAdd, ConcurrentMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie) {
      this.countOfItemsToAdd = countOfItemsToAdd;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
    }

    @Override
    public Void call() {
      try {
        for (int i = 0; i < countOfItemsToAdd; i++) {
          final byte[] key = generateBigKey(random);
          final byte[] value = generateBigValue(random);

          final ByteHolder holderKey = new ByteHolder(key);
          final Lock lock = striped.get(holderKey);
          lock.lock();
          try {
            data.put(holderKey, new ByteHolder(value));
            lsmTrie.put(key, value);
          } finally {
            lock.unlock();
          }

          if (i > 0 & i % 100_000 == 0) {
            System.out.printf("%d writer thread, %d items processed\n", Thread.currentThread().getId(), i);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }

  private final static class WriterMixedKeys implements Callable<Void> {
    private final int                                   countOfItemsToAdd;
    private final ConcurrentMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                         striped;
    private final OLSMTrie                              lsmTrie;
    private final ThreadLocalRandom                     random = ThreadLocalRandom.current();

    WriterMixedKeys(int countOfItemsToAdd, ConcurrentMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie) {
      this.countOfItemsToAdd = countOfItemsToAdd;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
    }

    @Override
    public Void call() {
      try {
        for (int i = 0; i < countOfItemsToAdd; i++) {
          final byte[] key = generateMixedKey(random);
          final byte[] value = generateMixedValue(random);

          final ByteHolder holderKey = new ByteHolder(key);
          final Lock lock = striped.get(holderKey);
          lock.lock();
          try {
            data.put(holderKey, new ByteHolder(value));
            lsmTrie.put(key, value);
          } finally {
            lock.unlock();
          }

          if (i > 0 & i % 100_000 == 0) {
            System.out.printf("%d writer thread, %d items processed\n", Thread.currentThread().getId(), i);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }

  private static final class Reader implements Callable<Void> {
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;

    Reader(ConcurrentSkipListMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie, AtomicBoolean stop) {
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
    }

    @Override
    public Void call() {
      int counter = 0;
      try {
        while (!stop.get()) {
          ByteHolder existingKey = null;

          while (!stop.get()) {
            final ByteHolder key = new ByteHolder(generateKey(random));

            existingKey = data.ceilingKey(key);
            if (existingKey == null) {
              existingKey = data.floorKey(key);
            }
            if (existingKey != null) {
              break;
            }
          }

          if (existingKey == null) {
            continue;
          }

          final Lock lock = striped.get(existingKey);
          lock.lock();
          try {
            final ByteHolder value = data.get(existingKey);
            Assert.assertNotNull(value);
            final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
            Assert.assertNotNull(lsmValue);
            Assert.assertEquals(value, new ByteHolder(lsmValue));
            counter++;

            if (counter > 0 & counter % 100_000 == 0) {
              System.out.printf("%d reader thread, %d items processed\n", Thread.currentThread().getId(), counter);
            }

          } finally {
            lock.unlock();
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private static final class ReaderBigKeys implements Callable<Void> {
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;

    ReaderBigKeys(ConcurrentSkipListMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie, AtomicBoolean stop) {
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
    }

    @Override
    public Void call() {
      int counter = 0;
      try {
        while (!stop.get()) {
          ByteHolder existingKey = null;

          while (!stop.get()) {
            final ByteHolder key = new ByteHolder(generateBigKey(random));

            existingKey = data.ceilingKey(key);
            if (existingKey == null) {
              existingKey = data.floorKey(key);
            }
            if (existingKey != null) {
              break;
            }
          }

          if (existingKey == null) {
            continue;
          }

          final Lock lock = striped.get(existingKey);
          lock.lock();
          try {
            final ByteHolder value = data.get(existingKey);
            Assert.assertNotNull(value);
            final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
            Assert.assertNotNull(lsmValue);
            Assert.assertEquals(value, new ByteHolder(lsmValue));
            counter++;

            if (counter > 0 & counter % 100_000 == 0) {
              System.out.printf("%d reader thread, %d items processed\n", Thread.currentThread().getId(), counter);
            }

          } finally {
            lock.unlock();
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private static final class ReaderMixedKeys implements Callable<Void> {
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;

    ReaderMixedKeys(ConcurrentSkipListMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie,
        AtomicBoolean stop) {
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
    }

    @Override
    public Void call() {
      int counter = 0;
      try {
        while (!stop.get()) {
          ByteHolder existingKey = null;

          while (!stop.get()) {
            final ByteHolder key = new ByteHolder(generateMixedKey(random));

            existingKey = data.ceilingKey(key);
            if (existingKey == null) {
              existingKey = data.floorKey(key);
            }
            if (existingKey != null) {
              break;
            }
          }

          if (existingKey == null) {
            continue;
          }

          final Lock lock = striped.get(existingKey);
          lock.lock();
          try {
            final ByteHolder value = data.get(existingKey);
            Assert.assertNotNull(value);
            final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
            Assert.assertNotNull(lsmValue);
            Assert.assertEquals(value, new ByteHolder(lsmValue));
            counter++;

            if (counter > 0 & counter % 100_000 == 0) {
              System.out.printf("%d reader thread, %d items processed\n", Thread.currentThread().getId(), counter);
            }

          } finally {
            lock.unlock();
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private static final class Modifier implements Callable<Void> {
    private final int                                           minSize;
    private final int                                           sizeLimit;
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;
    private final AtomicInteger                                 sizeCounter;

    private Modifier(int minSize, int sizeLimit, AtomicInteger sizeCounter, ConcurrentSkipListMap<ByteHolder, ByteHolder> data,
        Striped<Lock> striped, OLSMTrie lsmTrie, AtomicBoolean stop) {
      this.minSize = minSize;
      this.sizeLimit = sizeLimit;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
      this.sizeCounter = sizeCounter;
    }

    @Override
    public Void call() {
      try {
        boolean sizeLimitReached = false;
        long add = 0;
        long read = 0;
        long update = 0;

        while (!stop.get()) {

          if ((add + update + read) > 0 && (add + update + read) % 500_000 == 0) {
            System.out.printf("Thread %d, %,d operations were performed (%,d reads, %,d updates, %,d additions), db size is %,d\n",
                Thread.currentThread().getId(), (add + +update + read), read, update, add, sizeCounter.get());
          }

          if (!sizeLimitReached) {
            final int size = sizeCounter.get();

            sizeLimitReached = size >= sizeLimit;

            if (sizeLimitReached) {
              System.out.printf("Thread %d: size limit %d is reached\n", Thread.currentThread().getId(), size);
            }
            if (size < minSize) {
              add();
              add++;
              continue;
            }
          }

          final double operation = random.nextDouble();

          if (!sizeLimitReached) {
            if (operation < 0.3) {
              read();
              read++;
            } else if (operation < 0.6) {
              add();
              add++;
            } else {
              update();
              update++;
            }
          } else {
            if (operation < 0.5) {
              update();
              update++;
            } else {
              read();
              read++;
            }
          }

        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }

    private void read() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final ByteHolder value = data.get(existingKey);
        Assert.assertNotNull(value);
        final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
        Assert.assertNotNull(lsmValue);
        Assert.assertEquals(value, new ByteHolder(lsmValue));
      } finally {
        lock.unlock();
      }
    }

    private void add() {
      final byte[] key = generateKey(random);
      final byte[] value = generateValue(random);

      final ByteHolder holderKey = new ByteHolder(key);
      final Lock lock = striped.get(holderKey);
      lock.lock();
      try {
        ByteHolder oldValue = data.put(holderKey, new ByteHolder(value));
        lsmTrie.put(key, value);
        if (oldValue == null) {
          sizeCounter.getAndIncrement();
        }
      } finally {
        lock.unlock();
      }
    }

    private void update() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final byte[] value = generateValue(random);
        final ByteHolder valueHolder = new ByteHolder(value);

        data.put(existingKey, valueHolder);
        lsmTrie.put(existingKey.bytes, value);
      } finally {
        lock.unlock();
      }
    }
  }

  private static final class ModifierBigKeys implements Callable<Void> {
    private final int                                           minSize;
    private final int                                           sizeLimit;
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;
    private final AtomicInteger                                 sizeCounter;

    private ModifierBigKeys(int minSize, int sizeLimit, AtomicInteger sizeCounter,
        ConcurrentSkipListMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie, AtomicBoolean stop) {
      this.minSize = minSize;
      this.sizeLimit = sizeLimit;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
      this.sizeCounter = sizeCounter;
    }

    @Override
    public Void call() {
      try {
        boolean sizeLimitReached = false;
        long add = 0;
        long read = 0;
        long update = 0;

        while (!stop.get()) {

          if ((add + update + read) > 0 && (add + update + read) % 500_000 == 0) {
            System.out.printf("Thread %d, %,d operations were performed (%,d reads, %,d updates, %,d additions), db size is %,d\n",
                Thread.currentThread().getId(), (add + +update + read), read, update, add, sizeCounter.get());
          }

          if (!sizeLimitReached) {
            final int size = sizeCounter.get();

            sizeLimitReached = size >= sizeLimit;

            if (sizeLimitReached) {
              System.out.printf("Thread %d: size limit %d is reached\n", Thread.currentThread().getId(), size);
            }
            if (size < minSize) {
              add();
              add++;
              continue;
            }
          }

          final double operation = random.nextDouble();

          if (!sizeLimitReached) {
            if (operation < 0.3) {
              read();
              read++;
            } else if (operation < 0.6) {
              add();
              add++;
            } else {
              update();
              update++;
            }
          } else {
            if (operation < 0.5) {
              update();
              update++;
            } else {
              read();
              read++;
            }
          }

        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }

    private void read() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateBigKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final ByteHolder value = data.get(existingKey);
        Assert.assertNotNull(value);
        final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
        Assert.assertNotNull(lsmValue);
        Assert.assertEquals(value, new ByteHolder(lsmValue));
      } finally {
        lock.unlock();
      }
    }

    private void add() {
      final byte[] key = generateBigKey(random);
      final byte[] value = generateBigValue(random);

      final ByteHolder holderKey = new ByteHolder(key);
      final Lock lock = striped.get(holderKey);
      lock.lock();
      try {
        ByteHolder oldValue = data.put(holderKey, new ByteHolder(value));
        lsmTrie.put(key, value);
        if (oldValue == null) {
          sizeCounter.getAndIncrement();
        }
      } finally {
        lock.unlock();
      }
    }

    private void update() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateBigKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final byte[] value = generateBigValue(random);
        final ByteHolder valueHolder = new ByteHolder(value);

        data.put(existingKey, valueHolder);
        lsmTrie.put(existingKey.bytes, value);
      } finally {
        lock.unlock();
      }
    }
  }

  private static final class ModifierMixedKeys implements Callable<Void> {
    private final int                                           minSize;
    private final int                                           sizeLimit;
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom                             random = ThreadLocalRandom.current();
    private final AtomicBoolean                                 stop;
    private final AtomicInteger                                 sizeCounter;

    private ModifierMixedKeys(int minSize, int sizeLimit, AtomicInteger sizeCounter,
        ConcurrentSkipListMap<ByteHolder, ByteHolder> data, Striped<Lock> striped, OLSMTrie lsmTrie, AtomicBoolean stop) {
      this.minSize = minSize;
      this.sizeLimit = sizeLimit;
      this.data = data;
      this.striped = striped;
      this.lsmTrie = lsmTrie;
      this.stop = stop;
      this.sizeCounter = sizeCounter;
    }

    @Override
    public Void call() {
      try {
        boolean sizeLimitReached = false;
        long add = 0;
        long read = 0;
        long update = 0;

        while (!stop.get()) {

          if ((add + update + read) > 0 && (add + update + read) % 500_000 == 0) {
            System.out.printf("Thread %d, %,d operations were performed (%,d reads, %,d updates, %,d additions), db size is %,d\n",
                Thread.currentThread().getId(), (add + +update + read), read, update, add, sizeCounter.get());
          }

          if (!sizeLimitReached) {
            final int size = sizeCounter.get();

            sizeLimitReached = size >= sizeLimit;

            if (sizeLimitReached) {
              System.out.printf("Thread %d: size limit %d is reached\n", Thread.currentThread().getId(), size);
            }
            if (size < minSize) {
              add();
              add++;
              continue;
            }
          }

          final double operation = random.nextDouble();

          if (!sizeLimitReached) {
            if (operation < 0.3) {
              read();
              read++;
            } else if (operation < 0.6) {
              add();
              add++;
            } else {
              update();
              update++;
            }
          } else {
            if (operation < 0.5) {
              update();
              update++;
            } else {
              read();
              read++;
            }
          }

        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }

    private void read() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateMixedKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final ByteHolder value = data.get(existingKey);
        Assert.assertNotNull(value);
        final byte[] lsmValue = lsmTrie.get(existingKey.bytes);
        Assert.assertNotNull(lsmValue);
        Assert.assertEquals(value, new ByteHolder(lsmValue));
      } finally {
        lock.unlock();
      }
    }

    private void add() {
      final byte[] key = generateMixedKey(random);
      final byte[] value = generateMixedValue(random);

      final ByteHolder holderKey = new ByteHolder(key);
      final Lock lock = striped.get(holderKey);
      lock.lock();
      try {
        ByteHolder oldValue = data.put(holderKey, new ByteHolder(value));
        lsmTrie.put(key, value);
        if (oldValue == null) {
          sizeCounter.getAndIncrement();
        }
      } finally {
        lock.unlock();
      }
    }

    private void update() {
      ByteHolder existingKey = null;

      while (!stop.get()) {
        final ByteHolder key = new ByteHolder(generateMixedKey(random));

        existingKey = data.ceilingKey(key);
        if (existingKey == null) {
          existingKey = data.floorKey(key);
        }
        if (existingKey != null) {
          break;
        }
      }

      if (existingKey == null) {
        return;
      }

      final Lock lock = striped.get(existingKey);
      lock.lock();
      try {
        final byte[] value = generateMixedValue(random);
        final ByteHolder valueHolder = new ByteHolder(value);

        data.put(existingKey, valueHolder);
        lsmTrie.put(existingKey.bytes, value);
      } finally {
        lock.unlock();
      }
    }
  }
}
