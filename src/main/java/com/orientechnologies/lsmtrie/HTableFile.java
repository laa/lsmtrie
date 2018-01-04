package com.orientechnologies.lsmtrie;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

class HTableFile {
  private final Path        bloomFilterPath;
  private final Path        htablePath;

  HTableFile(Path bloomFilterPath, Path htablePath) {
    this.bloomFilterPath = bloomFilterPath;
    this.htablePath = htablePath;
  }

  public Path getHtablePath() {
    return htablePath;
  }

  public Path getBloomFilterPath() {
    return bloomFilterPath;
  }
}
