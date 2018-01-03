package com.orientechnologies.lsmtrie;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

class HTableFileChannel {
  private final Path        bloomFilterPath;
  private final Path        htablePath;
  private final FileChannel channel;

  HTableFileChannel(Path bloomFilterPath, Path htablePath, FileChannel channel) {
    this.bloomFilterPath = bloomFilterPath;
    this.htablePath = htablePath;
    this.channel = channel;
  }

  public Path getHtablePath() {
    return htablePath;
  }

  public FileChannel getChannel() {
    return channel;
  }

  public Path getBloomFilterPath() {
    return bloomFilterPath;
  }
}
