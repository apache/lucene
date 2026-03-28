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
import java.util.concurrent.locks.ReentrantLock;

/**
 * For managing multiple instances of {@link IndexWriter} sharing the same buffer (configured by
 * {@link IndexWriterConfig#setRAMBufferSizeMB})
 */
public class IndexWriterRAMManager {
  private final LinkedIdToWriter idToWriter = new LinkedIdToWriter();
  private final AtomicInteger idGenerator = new AtomicInteger();
  private double ramBufferSizeMB;

  /**
   * Default constructor
   *
   * @param ramBufferSizeMB the RAM buffer size to use between all registered {@link IndexWriter}
   *     instances
   */
  public IndexWriterRAMManager(double ramBufferSizeMB) {
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0) {
      throw new IllegalArgumentException("ramBufferSize should be > 0.0 MB when enabled");
    }
    this.ramBufferSizeMB = ramBufferSizeMB;
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
   * and eventually loop back to the beginning. Returns the flushed writer id for testing
   */
  public int flushRoundRobin() throws IOException {
    return idToWriter.flushRoundRobin();
  }

  /** Gets the number of writers registered with this ram manager */
  public int getWriterCount() {
    return idToWriter.size();
  }

  /** Registers a writer can returns the associated ID */
  protected int registerWriter(IndexWriter writer) {
    int id = idGenerator.incrementAndGet();
    idToWriter.addWriter(writer, id);
    return id;
  }

  /** Removes a writer given the writer's ide, protected for testing */
  protected void removeWriter(int id) {
    idToWriter.removeWriter(id);
  }

  /**
   * Will call {@link IndexWriter#ramBytesUsed()} for the writer id passed in, and then updates the
   * total ram using that value and returns it
   */
  public long updateAndGetCurrentBytesUsed(int id) {
    return idToWriter.getTotalRamTracker(id);
  }

  private static class LinkedIdToWriter {
    private final Map<Integer, IndexWriterNode> idToWriterNode = new HashMap<>();
    private IndexWriterNode first;
    private IndexWriterNode last;
    private long totalRamTracker;

    private final ReentrantLock lock = new ReentrantLock();

    // for round-robin flushing
    private int lastIdFlushed = -1;

    void addWriter(IndexWriter writer, int id) {
      synchronized (lock) {
        IndexWriterNode node = new IndexWriterNode(writer, id);
        if (idToWriterNode.isEmpty()) {
          first = node;
          last = node;
        }
        node.next = first;
        last.next = node;
        node.prev = last;
        last = node;
        first.prev = node;
        idToWriterNode.put(id, node);
      }
    }

    void removeWriter(int id) {
      synchronized (lock) {
        if (idToWriterNode.containsKey(id)) {
          IndexWriterNode nodeToRemove = idToWriterNode.remove(id);
          totalRamTracker -= nodeToRemove.ram;
          if (idToWriterNode.isEmpty()) {
            first = null;
            last = null;
            lastIdFlushed = -1;
            return;
          }
          if (id == lastIdFlushed) {
            lastIdFlushed = nodeToRemove.prev.id;
          }
          nodeToRemove.prev.next = nodeToRemove.next;
          nodeToRemove.next.prev = nodeToRemove.prev;
          if (nodeToRemove == first) {
            first = nodeToRemove.next;
          }
          if (nodeToRemove == last) {
            last = nodeToRemove.prev;
          }
        }
      }
    }

    // Returns the writer id that we attempted to flush (for testing purposes)
    int flushRoundRobin() throws IOException {
      synchronized (lock) {
        if (idToWriterNode.isEmpty()) {
          return -1;
        }
        int idToFlush;
        if (lastIdFlushed == -1) {
          idToFlush = first.id;
        } else {
          idToFlush = idToWriterNode.get(lastIdFlushed).next.id;
        }
        idToWriterNode.get(idToFlush).writer.flushNextBuffer();
        lastIdFlushed = idToFlush;
        return idToFlush;
      }
    }

    long getTotalRamTracker(int id) {
      synchronized (lock) {
        if (idToWriterNode.isEmpty()) {
          return 0;
        }
        if (idToWriterNode.containsKey(id) == false) {
          return totalRamTracker;
        }
        long oldRAMBytesUsed = idToWriterNode.get(id).ram;
        long newRAMBytesUsed = idToWriterNode.get(id).writer.ramBytesUsed();
        idToWriterNode.get(id).ram = newRAMBytesUsed;
        totalRamTracker += newRAMBytesUsed - oldRAMBytesUsed;
        return totalRamTracker;
      }
    }

    int size() {
      synchronized (lock) {
        return idToWriterNode.size();
      }
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
