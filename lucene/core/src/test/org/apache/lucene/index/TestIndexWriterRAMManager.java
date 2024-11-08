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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexWriterRAMManager extends LuceneTestCase {

  private static final FieldType storedTextType = new FieldType(TextField.TYPE_NOT_STORED);

  public void testSingleWriter() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
      TestFlushPolicy flushPolicy = new TestFlushPolicy();
      indexWriterConfig.setFlushPolicy(flushPolicy);
      indexWriterConfig.setRAMBufferSizeMB(1);
      try (IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
        assertEquals(0, w.ramBytesUsed());
        int i = 0;
        int errorLimit = 100000; // prevents loop from iterating forever
        while (flushPolicy.flushedWriters.isEmpty()) {
          Document doc = new Document();
          doc.add(newField("id", String.valueOf(i), storedTextType));
          w.addDocument(doc);
          i += 1;
          if (i == errorLimit) {
            fail("Writer has not flushed when expected");
          }
        }
        assertEquals(1, flushPolicy.flushedWriters.size());
        // suppresses null pointer warning in the next line, this will always return true
        assert flushPolicy.flushedWriters.size() == 1;
        assertEquals(1, (int) flushPolicy.flushedWriters.poll());
      }
    }
  }

  public void testMultipleWriters() throws IOException {
    try (Directory dir = newDirectory()) {
      try (Directory dir2 = newDirectory()) {
        IndexWriterRAMManager indexWriterRAMManager = new IndexWriterRAMManager(1);
        // Writers share the same buffer, so we pass in the same ram manager
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(indexWriterRAMManager);
        TestFlushPolicy flushPolicy = new TestFlushPolicy();
        indexWriterConfig.setFlushPolicy(flushPolicy);
        IndexWriterConfig indexWriterConfig2 = new IndexWriterConfig(indexWriterRAMManager);
        indexWriterConfig2.setFlushPolicy(flushPolicy);

        try (IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
          try (IndexWriter w2 = new IndexWriter(dir2, indexWriterConfig2)) {
            assertEquals(0, w.ramBytesUsed());
            assertEquals(0, w2.ramBytesUsed());
            int i = 0;
            int errorLimit = 100000; // prevents loop from iterating forever
            boolean addToWriter1 = true;
            while (flushPolicy.flushedWriters.size() < 2) {
              Document doc = new Document();
              doc.add(newField("id", String.valueOf(i), storedTextType));
              if (addToWriter1) {
                w.addDocument(doc);
              } else {
                w2.addDocument(doc);
              }
              addToWriter1 = !addToWriter1;
              i += 1;
              if (i == errorLimit) {
                fail("Writers have not flushed when expected");
              }
            }
            assertEquals(2, flushPolicy.flushedWriters.size());
            assert flushPolicy.flushedWriters.size() == 2;
            assertEquals(1, (int) flushPolicy.flushedWriters.poll());
            assert flushPolicy.flushedWriters.size() == 1;
            assertEquals(2, (int) flushPolicy.flushedWriters.poll());
          }
        }
      }
    }
  }

  public void testMultipleWritersWithRemoval() throws IOException {
    try (Directory dir = newDirectory()) {
      try (Directory dir2 = newDirectory()) {
        IndexWriterRAMManager indexWriterRAMManager = new IndexWriterRAMManager(1);
        // Writers share the same buffer, so we pass in the same ram manager
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(indexWriterRAMManager);
        TestFlushPolicy flushPolicy = new TestFlushPolicy();
        indexWriterConfig.setFlushPolicy(flushPolicy);
        IndexWriterConfig indexWriterConfig2 = new IndexWriterConfig(indexWriterRAMManager);
        indexWriterConfig2.setFlushPolicy(flushPolicy);

        IndexWriter w = new IndexWriter(dir, indexWriterConfig);
        IndexWriter w2 = new IndexWriter(dir2, indexWriterConfig2);
        assertEquals(0, w.ramBytesUsed());
        assertEquals(0, w2.ramBytesUsed());
        int i = 0;
        int errorLimit = 100000; // prevents loop from iterating forever
        boolean addToWriter1 = true;
        boolean w2Close = false;
        while (flushPolicy.flushedWriters.size() < 4) {
          if (w2Close == false && flushPolicy.flushedWriters.size() == 2) {
            w2.close();
            w2Close = true;
          }
          Document doc = new Document();
          doc.add(newField("id", String.valueOf(i), storedTextType));
          if (addToWriter1 || w2Close) {
            w.addDocument(doc);
          } else {
            w2.addDocument(doc);
          }
          addToWriter1 = !addToWriter1;
          i += 1;
          if (i == errorLimit) {
            if (w.isOpen()) {
              w.close();
              ;
            }
            if (w2.isOpen()) {
              w2.close();
            }
            w2.close();
            fail("Writers have not flushed when expected");
          }
        }
        if (w.isOpen()) {
          w.close();
          ;
        }
        if (w2.isOpen()) {
          w2.close();
        }
        // we expect 1 flushed, then 2, then 1 then 1 since 2 was removed
        assertEquals(4, flushPolicy.flushedWriters.size());
        assert flushPolicy.flushedWriters.size() == 4;
        assertEquals(1, (int) flushPolicy.flushedWriters.poll());
        assert flushPolicy.flushedWriters.size() == 3;
        assertEquals(2, (int) flushPolicy.flushedWriters.poll());
        assert flushPolicy.flushedWriters.size() == 2;
        assertEquals(1, (int) flushPolicy.flushedWriters.poll());
        assert flushPolicy.flushedWriters.size() == 1;
        assertEquals(1, (int) flushPolicy.flushedWriters.poll());
      }
    }
  }

  public void testMultipleWritersWithAdding() throws IOException {
    try (Directory dir = newDirectory()) {
      try (Directory dir2 = newDirectory()) {
        try (Directory dir3 = newDirectory()) {
          IndexWriterRAMManager indexWriterRAMManager = new IndexWriterRAMManager(1);
          // Writers share the same buffer, so we pass in the same ram manager
          IndexWriterConfig indexWriterConfig = new IndexWriterConfig(indexWriterRAMManager);
          TestFlushPolicy flushPolicy = new TestFlushPolicy();
          indexWriterConfig.setFlushPolicy(flushPolicy);
          IndexWriterConfig indexWriterConfig2 = new IndexWriterConfig(indexWriterRAMManager);
          indexWriterConfig2.setFlushPolicy(flushPolicy);
          IndexWriterConfig indexWriterConfig3 = new IndexWriterConfig(indexWriterRAMManager);
          indexWriterConfig3.setFlushPolicy(flushPolicy);

          IndexWriter w = new IndexWriter(dir, indexWriterConfig);
          IndexWriter w2 = new IndexWriter(dir2, indexWriterConfig2);
          IndexWriter w3 = null; // don't init this right now
          assertEquals(0, w.ramBytesUsed());
          assertEquals(0, w2.ramBytesUsed());
          int i = 0;
          int errorLimit = 100000; // prevents loop from iterating forever
          while (flushPolicy.flushedWriters.size() < 5) {
            if (w3 == null && flushPolicy.flushedWriters.size() == 3) {
              w3 = new IndexWriter(dir3, indexWriterConfig3);
              assertEquals(0, w3.ramBytesUsed());
            }
            Document doc = new Document();
            doc.add(newField("id", String.valueOf(i), storedTextType));
            if (i % 3 == 0) {
              w.addDocument(doc);
            } else if (i % 3 == 1) {
              w2.addDocument(doc);
            } else if (w3 != null) {
              w3.addDocument(doc);
            }
            i += 1;
            if (i == errorLimit) {
              if (w.isOpen()) {
                w.close();
                ;
              }
              if (w2.isOpen()) {
                w2.close();
              }
              if (w3 != null && w3.isOpen()) {
                w3.close();
              }
              w2.close();
              fail("Writers have not flushed when expected");
            }
          }
          if (w.isOpen()) {
            w.close();
            ;
          }
          if (w2.isOpen()) {
            w2.close();
          }
          if (w3 != null && w3.isOpen()) {
            w3.close();
          }
          // we expect 1 flushed, then 2, then 1, then 2, then 3 since 3 was added
          assertEquals(5, flushPolicy.flushedWriters.size());
          assert flushPolicy.flushedWriters.size() == 5;
          assertEquals(1, (int) flushPolicy.flushedWriters.poll());
          assert flushPolicy.flushedWriters.size() == 4;
          assertEquals(2, (int) flushPolicy.flushedWriters.poll());
          assert flushPolicy.flushedWriters.size() == 3;
          assertEquals(1, (int) flushPolicy.flushedWriters.poll());
          assert flushPolicy.flushedWriters.size() == 2;
          assertEquals(2, (int) flushPolicy.flushedWriters.poll());
          assert flushPolicy.flushedWriters.size() == 1;
          assertEquals(3, (int) flushPolicy.flushedWriters.poll());
        }
      }
    }
  }

  public void testRandom() throws IOException {
    for (int i = 0; i < 20; i++) {
      randomTest();
    }
  }

  private static void randomTest() throws IOException {
    int numWriters = random().nextInt(1, 100);
    double ramBufferSize = random().nextDouble();
    List<Directory> directories = new ArrayList<>();
    List<IndexWriterConfig> configs = new ArrayList<>();
    List<IndexWriter> writers = new ArrayList<>();
    TestFlushPolicy flushPolicy = new TestFlushPolicy();
    TestEventRecordingIndexWriterRAMManager ramManager =
        new TestEventRecordingIndexWriterRAMManager(ramBufferSize);
    for (int i = 0; i < numWriters; i++) {
      directories.add(newDirectory());
      configs.add(new IndexWriterConfig(ramManager));
      configs.get(i).setFlushPolicy(flushPolicy);
      writers.add(new IndexWriter(directories.get(i), configs.get(i)));
      assertEquals(0, writers.get(i).ramBytesUsed());
    }

    int flushedLimit = numWriters * 2;
    int docId = 0;
    while (ramManager.flushCount.get() < flushedLimit) {
      boolean changeWriters = random().nextDouble() < 0.2;
      if (changeWriters) {
        boolean addWriter = random().nextBoolean();
        if (addWriter) {
          directories.add(newDirectory());
          configs.add(new IndexWriterConfig(ramManager).setFlushPolicy(flushPolicy));
          writers.add(new IndexWriter(directories.getLast(), configs.getLast()));
        } else {
          int closeWriter = random().nextInt(writers.size());
          if (writers.get(closeWriter).isOpen()) {
            writers.get(closeWriter).close();
            directories.get(closeWriter).close();
          }
        }
      } else {
        Document doc = new Document();
        doc.add(newField("id", String.valueOf(docId++), storedTextType));
        int addDocWriter = random().nextInt(writers.size());
        if (writers.get(addDocWriter).isOpen()) {
          writers.get(addDocWriter).addDocument(doc);
        }
      }
    }

    verifyEvents(ramManager.events);

    for (int i = 0; i < writers.size(); i++) {
      if (writers.get(i).isOpen()) {
        writers.get(i).close();
        directories.get(i).close();
      }
    }
  }

  private static void verifyEvents(
      Queue<TestEventRecordingIndexWriterRAMManager.TestEventAndId> events) {

    TestEventRecordingIndexWriterRAMManager.TestEventAndId event = events.poll();
    int lastFlush = -1;
    int maxValidWriter = 1;
    Set<Integer> removedWriters = new HashSet<>();
    while (event != null) {
      if (event.event.equals(TestEventRecordingIndexWriterRAMManager.TestEvent.REMOVE)) {
        removedWriters.add(event.id);
        while (removedWriters.contains(maxValidWriter)) {
          maxValidWriter--;
        }
        while (removedWriters.contains(lastFlush)) {
          if (lastFlush == 1) {
            lastFlush = maxValidWriter;
          } else {
            lastFlush--;
          }
        }
      } else if (event.event.equals(TestEventRecordingIndexWriterRAMManager.TestEvent.ADD)) {
        if (event.id > maxValidWriter) {
          maxValidWriter = event.id;
        }
      } else if (event.event.equals(TestEventRecordingIndexWriterRAMManager.TestEvent.FLUSH)) {
        int flushedId = event.id;
        assertFalse("Flushed ID after removing it", removedWriters.contains(flushedId));
        if (lastFlush == -1) {
          if (removedWriters.contains(1) == false) {
            assertEquals("Must start flushing at the first id", 1, flushedId);
          }
        } else {
          int nextValidFlush = lastFlush + 1;
          while (removedWriters.contains(nextValidFlush) || nextValidFlush > maxValidWriter) {
            if (nextValidFlush > maxValidWriter) {
              nextValidFlush = 1;
            } else {
              nextValidFlush++;
            }
          }

          assertEquals("Flushed in the wrong order", nextValidFlush, event.id);
        }
        lastFlush = flushedId;
      }
      event = events.poll();
    }
  }

  /**
   * Flush policy used for testing that keeps track of all the writers that were flushed and what
   * order they were flushed in
   */
  private static class TestFlushPolicy extends FlushPolicy {

    ConcurrentLinkedQueue<Integer> flushedWriters = new ConcurrentLinkedQueue<>();

    @Override
    public void onChange(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {}

    @Override
    public void flushWriter(
        IndexWriterRAMManager ramManager,
        IndexWriterRAMManager.PerWriterIndexWriterRAMManager perWriterRamManager)
        throws IOException {
      long totalBytes = perWriterRamManager.getTotalBufferBytesUsed();
      if (totalBytes > ramManager.getRamBufferSizeMB() * 1024 * 1024) {
        int flushedId = ramManager.flushRoundRobin();
        flushedWriters.add(flushedId);
      }
    }
  }

  private static class TestEventRecordingIndexWriterRAMManager extends IndexWriterRAMManager {

    public enum TestEvent {
      ADD,
      REMOVE,
      FLUSH;
    }

    record TestEventAndId(TestEvent event, int id) {

      @Override
      public String toString() {
        return event + " " + id;
      }
    }

    ConcurrentLinkedQueue<TestEventAndId> events = new ConcurrentLinkedQueue<>();

    AtomicInteger flushCount = new AtomicInteger();

    /**
     * Default constructor
     *
     * @param ramBufferSizeMB the RAM buffer size to use between all registered {@link IndexWriter}
     *     instances
     */
    TestEventRecordingIndexWriterRAMManager(double ramBufferSizeMB) {
      super(ramBufferSizeMB);
    }

    @Override
    public int flushRoundRobin() throws IOException {
      int flushed = super.flushRoundRobin();
      events.add(new TestEventAndId(TestEvent.FLUSH, flushed));
      flushCount.incrementAndGet();
      return flushed;
    }

    @Override
    protected int registerWriter(IndexWriter writer) {
      int id = super.registerWriter(writer);
      events.add(new TestEventAndId(TestEvent.ADD, id));
      return id;
    }

    @Override
    protected void removeWriter(int id) {
      super.removeWriter(id);
      events.add(new TestEventAndId(TestEvent.REMOVE, id));
    }
  }
}
