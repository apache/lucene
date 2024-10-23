/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For managing multiple instances of {@link IndexWriter} sharing the same buffer (configured by
 * {@link IndexWriterConfig#setRAMBufferSizeMB})
 */
public class IndexWriterRAMManager {

  private final IndexWriterConfig config;
  private final Map<Integer, IndexWriter> idToWriter = new ConcurrentHashMap<>();
  private final AtomicInteger idGenerator = new AtomicInteger();

  /**
   * Default constructor
   *
   * @param config the index writer config containing the max RAM buffer size
   */
  public IndexWriterRAMManager(IndexWriterConfig config) {
    this.config = config;
  }

  private int registerWriter(IndexWriter writer) {
    int id = idGenerator.incrementAndGet();
    idToWriter.put(id, writer);
    return id;
  }

  private void removeWriter(int id) {
    if (idToWriter.containsKey(id) == false) {
      throw new IllegalArgumentException(
          "Writer " + id + " has not been registered or has been removed already");
    }
    idToWriter.remove(id);
  }

  private void flushIfNecessary(int id) throws IOException {
    if (idToWriter.containsKey(id) == false) {
      throw new IllegalArgumentException(
          "Writer " + id + " has not been registered or has been removed already");
    }
    long totalRam = 0L;
    for (IndexWriter writer : idToWriter.values()) {
      totalRam += writer.ramBytesUsed();
    }
    if (totalRam >= config.getRAMBufferSizeMB() * 1024 * 1024) {
      IndexWriter writerToFlush = chooseWriterToFlush(idToWriter.values(), idToWriter.get(id));
      writerToFlush.flushNextBuffer();
    }
  }

  /**
   * Chooses which writer should be flushed. Default implementation chooses the writer with most RAM
   * usage
   *
   * @param writers list of writers registered with this {@link IndexWriterRAMManager}
   * @param callingWriter the writer calling {@link IndexWriterRAMManager#flushIfNecessary}
   * @return the IndexWriter to flush
   */
  protected static IndexWriter chooseWriterToFlush(
      Collection<IndexWriter> writers, IndexWriter callingWriter) {
    IndexWriter highestBufferWriter = null;
    long highestRam = 0L;
    for (IndexWriter writer : writers) {
      long writerRamBytes = writer.ramBytesUsed();
      if (writerRamBytes > highestRam) {
        highestRam = writerRamBytes;
        highestBufferWriter = writer;
      }
    }
    return highestBufferWriter;
  }

  /**
   * For use in {@link IndexWriter}, manages communication with the {@link IndexWriterRAMManager}
   */
  public static class PerWriterIndexWriterRAMManager {

    private final int id;
    private final IndexWriterRAMManager manager;

    PerWriterIndexWriterRAMManager(IndexWriter writer, IndexWriterRAMManager manager) {
      id = manager.registerWriter(writer);
      this.manager = manager;
    }

    void removeWriter() {
      manager.removeWriter(id);
    }

    void flushIfNecessary() throws IOException {
      manager.flushIfNecessary(id);
    }
  }
}
