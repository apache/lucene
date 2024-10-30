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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * For managing multiple instances of {@link IndexWriter} sharing the same buffer (configured by
 * {@link IndexWriterConfig#setRAMBufferSizeMB})
 */
public class IndexWriterRAMManager {
  private final LinkedIdToWriter idToWriter = new LinkedIdToWriter();
  private final AtomicInteger idGenerator = new AtomicInteger();
  private double ramBufferSizeMB;
  private final AtomicLong totalRamTracker;

  /**
   * Default constructor
   *
   * @param ramBufferSizeMB the RAM buffer size to use between all registered {@link IndexWriter}
   *     instances
   */
  IndexWriterRAMManager(double ramBufferSizeMB) {
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0) {
      throw new IllegalArgumentException("ramBufferSize should be > 0.0 MB when enabled");
    }
    this.ramBufferSizeMB = ramBufferSizeMB;
    this.totalRamTracker = new AtomicLong(0);
  }

  /** Set the buffer size for this manager */
  public void setRamBufferSizeMB(double ramBufferSizeMB) {
    this.ramBufferSizeMB = ramBufferSizeMB;
  }

  /** Get the buffer size assigned to this manager */
  public double getRamBufferSizeMB() {
    return ramBufferSizeMB;
  }

  /**
   * Calls {@link IndexWriter#flushNextBuffer()} in a round-robin fashion starting from the first
   * writer added that has not been removed yet. Subsequent calls will flush the next writer in line
   * and eventually loop back to the beginning.
   */
  public void flushRoundRobin() throws IOException {
    idToWriter.flushRoundRobin();
  }

  private int registerWriter(IndexWriter writer) {
    int id = idGenerator.incrementAndGet();
    idToWriter.addWriter(writer, id);
    return id;
  }

  private void removeWriter(int id) {
    idToWriter.removeWriter(id);
  }

  private void flushIfNecessary(
      FlushPolicy flushPolicy, PerWriterIndexWriterRAMManager perWriterRAMManager)
      throws IOException {
    flushPolicy.flushWriter(this, perWriterRAMManager);
  }

  private long updateAndGetCurrentBytesUsed(int id) {
    return totalRamTracker.addAndGet(idToWriter.updateRAMAndGetDifference(id));
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

    void flushIfNecessary(FlushPolicy flushPolicy) throws IOException {
      manager.flushIfNecessary(flushPolicy, this);
    }

    long getTotalBufferBytesUsed() {
      return manager.updateAndGetCurrentBytesUsed(id);
    }
  }

  private static class LinkedIdToWriter {
    private final Map<Integer, IndexWriterNode> idToWriterNode = new HashMap<>();
    private IndexWriterNode first;
    private IndexWriterNode last;

    private final ReentrantLock lock = new ReentrantLock();

    // for round-robin flushing
    private int lastIdFlushed = -1;

    void addWriter(IndexWriter writer, int id) {
      IndexWriterNode node = new IndexWriterNode(writer, id);
      lock.lock();
      if (idToWriterNode.isEmpty()) {
        first = node;
        last = node;
      }
      node.next = first;
      last.next = node;
      node.prev = last;
      last = node;
      idToWriterNode.put(id, node);
      lock.unlock();
    }

    void removeWriter(int id) {
      lock.lock();
      if (idToWriterNode.containsKey(id) == false) {
        throw new IllegalArgumentException(
            "Writer " + id + " has not been registered or has been removed already");
      }
      IndexWriterNode nodeToRemove = idToWriterNode.remove(id);
      if (idToWriterNode.isEmpty()) {
        first = null;
        last = null;
        return;
      }
      nodeToRemove.prev.next = nodeToRemove.next;
      nodeToRemove.next.prev = nodeToRemove.prev;
      if (nodeToRemove == first) {
        first = nodeToRemove.next;
      }
      if (nodeToRemove == last) {
        last = nodeToRemove.prev;
      }
      lock.unlock();
    }

    void flushRoundRobin() throws IOException {
      lock.lock();
      if (idToWriterNode.isEmpty()) {
        throw new IllegalCallerException("No registered writers");
      }
      int idToFlush;
      if (lastIdFlushed == -1) {
        idToFlush = first.id;
      } else {
        idToFlush = idToWriterNode.get(lastIdFlushed).next.id;
      }
      idToWriterNode.get(idToFlush).writer.flushNextBuffer();
      lastIdFlushed = idToFlush;
      lock.unlock();
    }

    long updateRAMAndGetDifference(int id) {
      lock.lock();
      long oldRAMBytesUsed = idToWriterNode.get(id).ram;
      long newRAMBytesUsed = idToWriterNode.get(id).writer.ramBytesUsed();
      lock.unlock();
      return newRAMBytesUsed - oldRAMBytesUsed;
    }

    private static class IndexWriterNode {
      IndexWriter writer;
      int id;
      long ram;
      IndexWriterNode next;
      IndexWriterNode prev;

      IndexWriterNode(IndexWriter writer, int id) {
        this.writer = writer;
        this.id = id;
        this.ram = writer.ramBytesUsed();
      }
    }
  }
}
