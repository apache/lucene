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
package org.apache.lucene.internal.tests;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTestSecrets extends LuceneTestCase {

  public void testCallerOfGetter() {
    final UnsupportedOperationException expected =
        expectThrows(UnsupportedOperationException.class, TestTestSecrets::illegalCaller);
    assertEquals(
        "Lucene TestSecrets can only be used by the test-framework.", expected.getMessage());
  }

  private static void illegalCaller() {
    TestSecrets.getIndexWriterAccess();
  }

  public void testCannotSet() {
    expectThrows(UnsupportedOperationException.class, () -> TestSecrets.setIndexWriterAccess(null));
    expectThrows(
        UnsupportedOperationException.class,
        () -> TestSecrets.setConcurrentMergeSchedulerAccess(null));
    expectThrows(
        UnsupportedOperationException.class, () -> TestSecrets.setIndexPackageAccess(null));
    expectThrows(
        UnsupportedOperationException.class, () -> TestSecrets.setSegmentReaderAccess(null));
  }

  public void testDeadlock() throws Exception {
    String javaHome = System.getProperty("java.home");
    String javaBin = Paths.get(javaHome, "bin", "java").toString();
    String classpath = System.getProperty("java.class.path");

    // Execute the DeadlockTest#main in a new JVM process (for isolation)
    ProcessBuilder builder =
        new ProcessBuilder(javaBin, "-cp", classpath, DeadlockTest.class.getName()).inheritIO();
    boolean finished;
    Process process = builder.start();
    // If the process doesn't finish within 10 seconds, consider it deadlocked
    finished = process.waitFor(10, TimeUnit.SECONDS);

    if (!finished) {
      process.destroyForcibly();
    }

    // If 'finished' is false, it means timeout (deadlock occurred)
    assertTrue("Deadlock occurred in TestSecrets", finished);
  }

  public static class DeadlockTest {

    public static void main() throws InterruptedException, IOException {
      System.out.println("Test started!");
      // Set a CyclicBarrier to make the two threads start as simultaneously as possible
      final CyclicBarrier barrier = new CyclicBarrier(2);

      Thread indexThread =
          new Thread(
              () -> {
                System.out.println("Thread #1 - Trigger to initialize IndexWriter");
                try {
                  barrier.await();
                  Class.forName("org.apache.lucene.index.IndexWriter");
                } catch (Exception e) {
                  System.err.println("Exception in Thread #1: " + e);
                }
              });

      Thread searchThread =
          new Thread(
              () -> {
                System.out.println("Thread #2 - Trigger to initialize SegmentReader");
                try {
                  barrier.await();
                  Class.forName("org.apache.lucene.index.SegmentReader");
                } catch (Exception e) {
                  System.err.println("Exception in Thread #2: " + e);
                }
              });

      indexThread.start();
      searchThread.start();

      try (Directory directory = new ByteBuffersDirectory()) {
        System.out.println("Created Directory object");
        try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
          System.out.println("Created IndexWriter object");
          indexWriter.addDocument(new Document());
          System.out.println("Called addDocument");
        }
      }
      indexThread.join();
      searchThread.join();
      System.out.println("Test finished!");
    }
  }
}
