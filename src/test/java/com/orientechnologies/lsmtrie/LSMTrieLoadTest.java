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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
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

public class LSMTrieLoadTest {
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

    int sec = 10 * 60 * 60;

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
    public int compareTo(ByteHolder other) {

      if (other.bytes.length == bytes.length) {
        for (int i = 0; i < bytes.length; i++) {
          final int cmp = Byte.compare(bytes[i], other.bytes[i]);

          if (cmp != 0) {
            return cmp;
          }
        }
      } else if (bytes.length > other.bytes.length) {
        return 1;
      } else if (bytes.length < other.bytes.length) {
        return -1;
      }

      return 0;
    }
  }

  private static final class Modifier implements Callable<Void> {
    private final int                                           minSize;
    private final int                                           sizeLimit;
    private final ConcurrentSkipListMap<ByteHolder, ByteHolder> data;
    private final Striped<Lock>                                 striped;
    private final OLSMTrie                                      lsmTrie;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicBoolean stop;
    private final AtomicInteger sizeCounter;

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

        long ops = 0;
        long start = System.nanoTime();
        long end = start;

        while (!stop.get()) {

          if ((add + update + read) > 0 && (add + update + read) % 500_000 == 0) {
            end = System.nanoTime();
            long diff = (add + update + read) - ops;

            long opsec = diff / ((end - start) / 1_000_000);

            start = end;
            ops += diff;

            System.out.printf("Thread %d, %,d operations were performed (%,d reads, %,d updates, %,d additions), db size is %,d ,"
                    + " speed is %,d op/ms\n", Thread.currentThread().getId(), (add + +update + read), read, update, add,
                sizeCounter.get(), opsec);
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

  private static byte[] generateValue(Random random) {
    final int valueSize = random.nextInt(30) + 15;
    final byte[] value = new byte[valueSize];
    random.nextBytes(value);
    return value;
  }

  private static byte[] generateKey(Random random) {
    final int keySize = random.nextInt(17) + 8;
    final byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

}
