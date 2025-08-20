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

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestMultiIndexMergeScheduler extends LuceneTestCase {

  public void testCloseSingle() throws Exception {
    Directory directory = newDirectory();
    HashSet<Directory> directoriesToBeClosed = new HashSet<>();
    directoriesToBeClosed.add(directory);

    MultiIndexMergeScheduler scheduler =
        new MultiIndexMergeScheduler(
            directory,
            new MultiIndexMergeScheduler.CombinedMergeScheduler() {
              @Override
              public void sync(Directory directory) {
                super.sync(directory);
                assertTrue(directoriesToBeClosed.remove(directory));
              }
            });
    scheduler.getCombinedMergeScheduler().setDefaultMaxMergesAndThreads(false);
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMergeScheduler(scheduler);
    iwc.setMaxBufferedDocs(2);
    IndexWriter writer = new IndexWriter(directory, iwc);

    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      Field idField = newStringField("id", "", Field.Store.YES);
      doc.add(idField);
      writer.addDocument(doc);
    }
    writer.commit();

    writer.forceMerge(1);
    writer.close();

    directory.close();
    assertTrue(directoriesToBeClosed.isEmpty());
  }

  @Timeout(millis = 1000 * 60 * 2)
  public void testCloseMultiple() throws Exception {
    // Write to many indexes and do merges on them.
    // The unit test is randomized but reproducible.
    int DIRECTORY_COUNT = 10;
    int DOCUMENT_COUNT = 50;

    // Create a new Directory for each index.
    var directories = ConcurrentHashMap.newKeySet();
    ArrayList<Directory> directoriesToBeClosed = new ArrayList<>();
    for (int i = 0; i < DIRECTORY_COUNT; i++) {
      Directory directory = newDirectory();
      directories.add(directory);
      directoriesToBeClosed.add(directory);
    }

    // This CombinedMergeScheduler setup code is only for unit testing.
    // Normally, the MultiIndexMergeScheduler would handle this setup automatically and silently.
    var combinedScheduler =
        new MultiIndexMergeScheduler.CombinedMergeScheduler() {
          @Override
          public void sync(Directory directory) {
            // Blocks and waits for one MultiIndexMergeScheduler's merges to finish.
            super.sync(directory);
            // Record success for this Directory for the unit test.
            assertTrue(directories.remove(directory));
          }
        };

    // Create a MultiIndexMergeScheduler and an IndexWriter for each Directory.
    ArrayList<MultiIndexMergeScheduler> schedulers = new ArrayList<>();
    ArrayList<IndexWriter> writers = new ArrayList<>();
    int[] documentCounts = new int[DIRECTORY_COUNT];
    int directoryIndex = 0;
    for (Directory directory : directoriesToBeClosed) {
      var scheduler = new MultiIndexMergeScheduler(directory, combinedScheduler);
      schedulers.add(scheduler);

      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setMergeScheduler(scheduler);
      iwc.setMaxBufferedDocs(2);

      IndexWriter writer = new IndexWriter(scheduler.getDirectory(), iwc);
      writers.add(writer);

      documentCounts[directoryIndex] = 0;
      ++directoryIndex;
    }

    assertTrue(schedulers.size() == DIRECTORY_COUNT);
    assertTrue(writers.size() == DIRECTORY_COUNT);

    // More unit test setup.
    schedulers.get(0).getCombinedMergeScheduler().setDefaultMaxMergesAndThreads(false);

    // Randomly spread the documents across the indexes.
    // The merge ordering is calculated beforehand to make this unit test be reproducible.
    // TODO: why is this a concurrent linked queue if all accesses are under synchronized(lock)?!
    ConcurrentLinkedQueue<Integer> nextDirectoryToWrite = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < DOCUMENT_COUNT; ++i) {
      int r = random().nextInt(DIRECTORY_COUNT);
      nextDirectoryToWrite.add(r);
      ++documentCounts[r];
    }

    // Ensure all threads have at least one document to work with.
    for (int i = 0; i < documentCounts.length; i++) {
      documentCounts[i] = Math.max(1, documentCounts[i]);
    }

    // Write documents to each index and force some merges.
    final AtomicBoolean stop = new AtomicBoolean();
    final ArrayList<Thread> threads = new ArrayList<>();
    final Object lock = new Object();
    for (int schedulerIndex = 0; schedulerIndex < schedulers.size(); schedulerIndex++) {
      int threadID = schedulerIndex;

      Thread thread =
          new Thread() {
            @Override
            public void run() {
              try {
                IndexWriter writer = writers.get(threadID);
                while (stop.get() == false) {
                  synchronized (lock) {
                    if (!nextDirectoryToWrite.isEmpty()
                        && nextDirectoryToWrite.peek() != threadID) {
                      // sleep
                      while (!nextDirectoryToWrite.isEmpty()
                          && nextDirectoryToWrite.peek() != threadID) {
                        try {
                          lock.wait();
                        } catch (InterruptedException e) {
                          stop.set(true);
                          throw new RuntimeException("subthread interrupted?", e);
                        }
                      }
                      // awaken
                      continue;
                    }

                    if (documentCounts[threadID] == 0) {
                      break;
                    }

                    // write a doc
                    Document doc = new Document();
                    Field idField = newStringField("id", "", Field.Store.NO);
                    doc.add(idField);
                    writer.addDocument(doc);
                    --documentCounts[threadID];

                    if (documentCounts[threadID] == 0) {
                      // force a merge
                      writer.commit();
                      writer.forceMerge(1, true);
                      writer.commit();
                      writer.close();
                      writers.set(threadID, null);
                    }

                    nextDirectoryToWrite.poll();
                    lock.notifyAll();
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };

      thread.setName("TestMultiIndexMergeScheduler thread-" + threadID);
      thread.start();
      threads.add(thread);
    }

    // Wait for the workers to finish.
    for (Thread thread : threads) {
      thread.join();
    }

    // Confirm that each IndexWriter wrote the expected number of documents and was closed.
    for (IndexWriter writer : writers) assertTrue(writer == null);

    // Confirm that each MultiIndexMergeScheduler was closed by its IndexWriter,
    // and also that the CombinedMergeScheduler waited for that IndexWriter's merges to finish.
    assertTrue(directories.isEmpty());

    // Clean up the Directory objects.
    for (Directory directory : directoriesToBeClosed) {
      directory.close();
    }
  }

  public void testReferenceCounting() throws Exception {
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() == null); // 0

    Directory directory1 = newDirectory();
    MultiIndexMergeScheduler mims1 = new MultiIndexMergeScheduler(directory1);
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() != null); // 1

    Directory directory2 = newDirectory();
    MultiIndexMergeScheduler mims2 = new MultiIndexMergeScheduler(directory2);
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() != null); // 2

    mims1.close();
    directory1.close();
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() != null); // 1

    mims2.close();
    directory2.close();
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() == null); // 0

    Directory directory3 = newDirectory();
    MultiIndexMergeScheduler mims3 = new MultiIndexMergeScheduler(directory3);
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() != null); // 1

    mims3.close();
    directory3.close();
    assertTrue(MultiIndexMergeScheduler.CombinedMergeScheduler.peekSingleton() == null); // 0
  }
}
