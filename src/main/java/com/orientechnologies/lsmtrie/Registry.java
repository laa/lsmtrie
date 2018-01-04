package com.orientechnologies.lsmtrie;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

public class Registry {
  private final Object lock = new Object();
  private long version;

  private FileChannel fileOneChannel;
  private FileChannel fileTwoChannel;

  private FileChannel current;

  private final Path fileOne;
  private final Path fileTwo;

  Registry(Path root, String name) {

    this.fileOne = root.resolve(name + "_v1.rg");
    this.fileTwo = root.resolve(name + "_v2.rg");
  }

  public void save(Node0 node0) throws IOException {
    synchronized (lock) {
      MetadataNode root = extractMetadata(node0);
      int contentSize = 8; /*version*/
      contentSize += calculateNodeContentSize(root);

      version++;
      ByteBuffer buffer = ByteBuffer.allocate(contentSize);
      buffer.putLong(version);
      serializeMetadata(root, null, -1, buffer);

      current.truncate(0);
      current.position(0);
      buffer.position(0);

      current.write(buffer);

      CRC32 crc32 = new CRC32();
      buffer.position(0);
      crc32.update(buffer);

      ByteBuffer crc32Buffer = ByteBuffer.allocate(4);
      crc32Buffer.putInt((int) crc32.getValue());
      crc32Buffer.position(0);

      current.write(crc32Buffer);
      current.force(true);

      if (current == fileOneChannel) {
        current = fileTwoChannel;
      } else {
        current = fileOneChannel;
      }

    }
  }

  private void serializeMetadata(MetadataNode node, MetadataNode parent, int parentIndex, ByteBuffer buffer) {
    final NodeMetadata metadata = node.metadata;

    if (parent == null) {
      buffer.putLong(-1);
      buffer.put((byte) 0);
    } else {
      buffer.putLong(parent.metadata.getId());
      buffer.put((byte) parentIndex);
    }

    buffer.putLong(metadata.getId());
    buffer.putInt(metadata.getLevel());

    long[] tableIds = metadata.getTableIds();
    String[] bloomFilters = metadata.getBloomFilterPaths();
    String[] htables = metadata.getHtablePaths();

    buffer.putInt(tableIds.length);
    for (int i = 0; i < tableIds.length; i++) {
      buffer.putLong(tableIds[i]);

      final byte[] htableFile = htables[i].getBytes(Charset.forName("UTF-8"));
      buffer.putInt(htableFile.length);
      buffer.put(htableFile);

      final byte[] bloomFilterFile = bloomFilters[i].getBytes(Charset.forName("UTF-8"));
      buffer.putInt(bloomFilterFile.length);
      buffer.put(bloomFilterFile);
    }

    for (int i = 0; i < node.children.size(); i++) {
      serializeMetadata(node.children.get(i), node, i, buffer);
    }
  }

  private MetadataNode extractMetadata(Node node) {
    MetadataNode rootNode = new MetadataNode(node.getMetadata());
    if (node.hasChildren()) {
      final NodeN[] children = node.getChildren();
      for (NodeN child : children) {
        rootNode.children.add(extractMetadata(child));
      }
    }

    return rootNode;
  }

  private int calculateNodeContentSize(MetadataNode metadataNode) {
    int contentSize = 8 + 8 + 1 + 4 + 4;/*parent id + node id + index in parent + level + tables count*/

    contentSize += metadataNode.metadata.getTableIds().length * 8;

    for (String bloomFilterName : metadataNode.metadata.getBloomFilterPaths()) {
      final byte[] encoded = bloomFilterName.getBytes(Charset.forName("UTF-8"));
      contentSize += encoded.length + 4;
    }

    for (String htableName : metadataNode.metadata.getHtablePaths()) {
      final byte[] encoded = htableName.getBytes(Charset.forName("UTF-8"));
      contentSize += encoded.length + 4;
    }

    for (MetadataNode child : metadataNode.children) {
      contentSize += calculateNodeContentSize(child);
    }

    return contentSize;
  }

  public Node0 load(AtomicLong nodeIdGen, AtomicLong tableIdGen, Semaphore compactionCounter) throws IOException {
    synchronized (lock) {
      fileOneChannel = FileChannel.open(fileOne, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
      fileTwoChannel = FileChannel.open(fileTwo, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

      current = fileOneChannel;

      if (fileOneChannel.size() == 0 && fileTwoChannel.size() == 0) {
        return new Node0(nodeIdGen.getAndIncrement(), nodeIdGen, compactionCounter);
      }

      final ByteBuffer buffer = readUpToDateData();

      final Map<Long, Node> idNodeMap = new HashMap<>();
      buffer.position(8);

      final Charset utf8 = Charset.forName("UTF-8");

      Node0 node0 = null;
      while (buffer.position() < buffer.limit()) {
        final long parentId = buffer.getLong();
        final int parentIndex = buffer.get();

        final long nodeId = buffer.getLong();
        if (nodeIdGen.get() <= nodeId) {
          nodeIdGen.set(nodeId + 1);
        }

        final int level = buffer.getInt();

        final int tablesCount = buffer.getInt();

        final Node node;
        if (parentId < 0) {
          node0 = new Node0(nodeId, nodeIdGen, compactionCounter);
          node = node0;
        } else {
          node = new NodeN(level, nodeId, nodeIdGen);

          final Node parent = idNodeMap.get(parentId);
          parent.setChild(parentIndex, (NodeN) node);
        }

        idNodeMap.put(nodeId, node);

        for (int i = 0; i < tablesCount; i++) {
          final long tableId = buffer.getLong();
          if (tableIdGen.get() <= tableId) {
            tableIdGen.set(tableId + 1);
          }

          final int htableSize = buffer.getInt();
          final byte[] htableBin = new byte[htableSize];
          buffer.get(htableBin);

          final int bloomFilterSize = buffer.getInt();
          final byte[] bloomFilterBin = new byte[bloomFilterSize];
          buffer.get(bloomFilterBin);

          @SuppressWarnings("unchecked")
          final BloomFilter<byte[]>[] bloomFilters = new BloomFilter[Table.BUCKETS_COUNT];

          final Path bloomFilterPath = Paths.get(new String(bloomFilterBin, utf8));
          try (FileChannel bloomChannel = FileChannel.open(bloomFilterPath, StandardOpenOption.READ)) {
            final InputStream inputStream = Channels.newInputStream(bloomChannel);
            for (int k = 0; k < bloomFilters.length; k++) {
              final BloomFilter<byte[]> bloomFilter = BloomFilter
                  .readFrom(inputStream, (Funnel<byte[]>) (bytes, primitiveSink) -> primitiveSink.putBytes(bytes));
              bloomFilters[k] = bloomFilter;
            }
          }

          final Path htablePath = Paths.get(new String(htableBin, utf8));
          final ByteBuffer htableBuffer;

          try (FileChannel htableChannel = FileChannel.open(htablePath, StandardOpenOption.READ)) {
            htableBuffer = htableChannel.map(FileChannel.MapMode.READ_ONLY, 0, htableChannel.size());
          }

          final HTableFile hTableFile = new HTableFile(bloomFilterPath, htablePath);
          final HTable hTable = new HTable(bloomFilters, htableBuffer, tableId);
          node.addHTable(hTable, hTableFile);
        }
      }

      return node0;
    }
  }

  private ByteBuffer readUpToDateData() throws IOException {
    final int bufferOneSize = (int) fileOneChannel.size() - 4;
    final int bufferTwoSize = (int) fileTwoChannel.size() - 4;

    ByteBuffer bufferOne;
    ByteBuffer bufferTwo;

    if (bufferOneSize > 0) {
      bufferOne = ByteBuffer.allocate(bufferOneSize);
    } else {
      bufferOne = null;
    }

    if (bufferTwoSize > 0) {
      bufferTwo = ByteBuffer.allocate(bufferTwoSize);
    } else {
      bufferTwo = null;
    }

    if (bufferOne != null) {
      fileOneChannel.position(0);
      fileOneChannel.read(bufferOne);
    }

    if (bufferTwo != null) {
      fileTwoChannel.position(0);
      fileTwoChannel.read(bufferTwo);
    }

    long bufferOneVersion = -1;
    long bufferTwoVersion = -1;

    if (bufferOne != null) {
      bufferOneVersion = bufferOne.getLong(0);
    }

    if (bufferTwo != null) {
      bufferTwoVersion = bufferTwo.getInt(0);
    }

    CRC32 crc32 = new CRC32();
    if (bufferOne != null) {
      bufferOne.position(0);
      crc32.update(bufferOne);

      ByteBuffer crc32Buffer = ByteBuffer.allocate(4);
      fileOneChannel.position(bufferOne.position());
      fileOneChannel.read(crc32Buffer);

      if (crc32Buffer.getInt(0) != (int) crc32.getValue()) {
        bufferOne = null;
        bufferOneVersion = -1;
      }
    }

    crc32.reset();

    if (bufferTwo != null) {
      bufferTwo.position(0);
      crc32.update(bufferTwo);

      ByteBuffer crc32Buffer = ByteBuffer.allocate(4);
      fileTwoChannel.position(bufferTwo.position());
      fileTwoChannel.read(crc32Buffer);

      if (crc32Buffer.getInt(0) != (int) crc32.getValue()) {
        bufferTwo = null;
        bufferTwoVersion = -1;
      }
    }

    ByteBuffer buffer;
    if (bufferOneVersion > bufferTwoVersion) {
      version = bufferOneVersion;
      buffer = bufferOne;
      current = fileTwoChannel;
    } else if (bufferTwoVersion == -1) {
      throw new IllegalStateException("Can not read trie configuration");
    } else {
      assert bufferTwoVersion > bufferOneVersion;
      version = bufferTwoVersion;
      buffer = bufferTwo;
      current = fileOneChannel;
    }

    return buffer;
  }

  public void close() throws IOException {
    synchronized (lock) {
      fileOneChannel.force(true);
      fileOneChannel.close();

      fileTwoChannel.force(true);
      fileTwoChannel.close();
    }
  }

  public void delete() throws IOException {
    synchronized (lock) {
      fileOneChannel.close();
      fileTwoChannel.close();

      Files.delete(fileOne);
      Files.delete(fileTwo);
    }
  }

  private final static class MetadataNode {
    private final List<MetadataNode> children = new ArrayList<>();
    private final NodeMetadata metadata;

    MetadataNode(NodeMetadata metadata) {
      this.metadata = metadata;
    }
  }
}
