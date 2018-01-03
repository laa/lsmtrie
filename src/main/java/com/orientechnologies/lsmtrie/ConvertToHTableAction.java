package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

class ConvertToHTableAction {
  private Path        bloomFilterPath;
  private Path        htablePath;
  private FileChannel htableChannel;
  private HTable      hTable;

  private final MemTable memTable;
  private final String   name;

  ConvertToHTableAction(MemTable memTable, String name) {
    this.memTable = memTable;
    this.name = name;
  }

  public Path getHtablePath() {
    return htablePath;
  }

  public FileChannel getHtableChannel() {
    return htableChannel;
  }

  public HTable gethTable() {
    return hTable;
  }

  public Path getBloomFilterPath() {
    return bloomFilterPath;
  }

  public ConvertToHTableAction invoke() throws IOException {
    final SerializedHTable serializedHTable = memTable.toHTable();
    final long tableId = memTable.getId();

    final String htableName = name + "_" + tableId + ".htb";
    final String bloomFiltersName = name + "_" + tableId + ".bl";

    bloomFilterPath = Paths.get(bloomFiltersName);
    final FileChannel bloomChannel = FileChannel
        .open(bloomFilterPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);

    try (final OutputStream outputStream = Channels.newOutputStream(bloomChannel)) {
      for (BloomFilter<byte[]> bloomFilter : serializedHTable.getBloomFilters()) {
        bloomFilter.writeTo(outputStream);
      }
    }
    bloomChannel.force(true);
    bloomChannel.close();

    htablePath = Paths.get(htableName);
    htableChannel = FileChannel.open(htablePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
    htableChannel.write(serializedHTable.getHtableBuffer());
    serializedHTable.free();
    htableChannel.force(true);

    hTable = new HTable(serializedHTable.getBloomFilters(),
        htableChannel.map(FileChannel.MapMode.READ_ONLY, 0, serializedHTable.getHtableSize()), tableId);
    return this;
  }
}
