package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class ConvertToHTableAction {
  private       HTable hTable;
  private final Path   root;

  private final MemTable memTable;
  private final String   name;
  private final int      level;

  ConvertToHTableAction(MemTable memTable, Path root, String name, int level) {
    this.memTable = memTable;
    this.root = root;
    this.name = name;
    this.level = level;
  }

  public HTable gethTable() {
    return hTable;
  }

  public ConvertToHTableAction invoke() throws IOException {
    final SerializedHTable serializedHTable = memTable.toHTable();
    final long tableId = memTable.getId();

    final String htableName = name + "_" + tableId + "_L_" + level + ".htb";
    final String bloomFiltersName = name + "_" + tableId + "_L_" + level + ".bl";

    final Path bloomFilterPath = root.resolve(bloomFiltersName);

    try (FileChannel bloomChannel = FileChannel
        .open(bloomFilterPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      final OutputStream outputStream = Channels.newOutputStream(bloomChannel);
      for (BloomFilter<byte[]> bloomFilter : serializedHTable.getBloomFilters()) {
        bloomFilter.writeTo(outputStream);
      }
    }

    Path htablePath = root.resolve(htableName);
    try (FileChannel htableChannel = FileChannel
        .open(htablePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {

      ByteBuffer buffer = serializedHTable.getHtableBuffer();
      buffer.position(0);

      int written = 0;
      final long htableSize = serializedHTable.getHtableSize();
      while (written < htableSize) {
        written += htableChannel.write(buffer);
      }
    }
    serializedHTable.free();

    try (FileChannel htableChannel = FileChannel.open(htablePath, StandardOpenOption.READ)) {
      if (htableChannel.size() != serializedHTable.getHtableSize()) {
        throw new IllegalStateException("Invalid size of file");
      }

      hTable = new HTable(serializedHTable.getBloomFilters(),
          htableChannel.map(FileChannel.MapMode.READ_ONLY, 0, serializedHTable.getHtableSize()), tableId, bloomFilterPath,
          htablePath);
    }

    return this;
  }
}
