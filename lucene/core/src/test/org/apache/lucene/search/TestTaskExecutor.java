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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTaskExecutor extends LuceneTestCase {

  private static ExecutorService executorService;

  @BeforeClass
  public static void createExecutor() {
    executorService =
        Executors.newFixedThreadPool(
            1, new NamedThreadFactory(TestTaskExecutor.class.getSimpleName()));
  }

  @AfterClass
  public static void shutdownExecutor() {
    executorService.shutdown();
  }

  public void testUnwrapIOExceptionFromExecutionException() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    IOException ioException =
        expectThrows(
            IOException.class,
            () ->
                taskExecutor.invokeAll(
                    Collections.singletonList(
                        () -> {
                          throw new IOException("io exception");
                        })));
    assertEquals("io exception", ioException.getMessage());
  }

  public void testUnwrapRuntimeExceptionFromExecutionException() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    RuntimeException runtimeException =
        expectThrows(
            RuntimeException.class,
            () ->
                taskExecutor.invokeAll(
                    Collections.singletonList(
                        () -> {
                          throw new RuntimeException("runtime");
                        })));
    assertEquals("runtime", runtimeException.getMessage());
    assertNull(runtimeException.getCause());
  }

  public void testUnwrapErrorFromExecutionException() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    OutOfMemoryError outOfMemoryError =
        expectThrows(
            OutOfMemoryError.class,
            () ->
                taskExecutor.invokeAll(
                    Collections.singletonList(
                        () -> {
                          throw new OutOfMemoryError("oom");
                        })));
    assertEquals("oom", outOfMemoryError.getMessage());
    assertNull(outOfMemoryError.getCause());
  }

  public void testUnwrappedExceptions() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    RuntimeException runtimeException =
        expectThrows(
            RuntimeException.class,
            () ->
                taskExecutor.invokeAll(
                    Collections.singletonList(
                        () -> {
                          throw new Exception("exc");
                        })));
    assertEquals("exc", runtimeException.getCause().getMessage());
  }

  public void testInvokeAllFromTaskDoesNotDeadlockSameSearcher() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 500; i++) {
        iw.addDocument(new Document());
      }
      try (DirectoryReader reader = iw.getReader()) {
        IndexSearcher searcher =
            new IndexSearcher(reader, executorService) {
              @Override
              protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                return slices(leaves, 1, 1);
              }
            };

        searcher.search(
            new MatchAllDocsQuery(),
            new CollectorManager<Collector, Void>() {
              @Override
              public Collector newCollector() {
                return new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context) {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) throws IOException {
                        searcher
                            .getTaskExecutor()
                            .invokeAll(
                                Collections.singletonList(
                                    () -> {
                                      // make sure that we don't miss disabling concurrency one
                                      // level deeper
                                      searcher
                                          .getTaskExecutor()
                                          .invokeAll(Collections.singletonList(() -> null));
                                      return null;
                                    }));
                      }

                      @Override
                      public void collect(int doc) {}
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE;
                  }
                };
              }

              @Override
              public Void reduce(Collection<Collector> collectors) {
                return null;
              }
            });
      }
    }
  }

  public void testInvokeAllFromTaskDoesNotDeadlockMultipleSearchers() throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 500; i++) {
        iw.addDocument(new Document());
      }
      try (DirectoryReader reader = iw.getReader()) {
        IndexSearcher searcher =
            new IndexSearcher(reader, executorService) {
              @Override
              protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                return slices(leaves, 1, 1);
              }
            };

        searcher.search(
            new MatchAllDocsQuery(),
            new CollectorManager<Collector, Void>() {
              @Override
              public Collector newCollector() {
                return new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context) {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) throws IOException {
                        // the thread local used to prevent deadlock is static, so while each
                        // searcher has its own
                        // TaskExecutor, the safeguard is shared among all the searchers that get
                        // the same executor
                        IndexSearcher indexSearcher = new IndexSearcher(reader, executorService);
                        indexSearcher
                            .getTaskExecutor()
                            .invokeAll(Collections.singletonList(() -> null));
                      }

                      @Override
                      public void collect(int doc) {}
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE;
                  }
                };
              }

              @Override
              public Void reduce(Collection<Collector> collectors) {
                return null;
              }
            });
      }
    }
  }

  public void testInvokeAllDoesNotLeaveTasksBehind() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    List<Callable<Void>> callables = new ArrayList<>();
    callables.add(
        () -> {
          throw new RuntimeException();
        });
    int tasksWithNormalExit = 99;
    for (int i = 0; i < tasksWithNormalExit; i++) {
      callables.add(
          () -> {
            tasksExecuted.incrementAndGet();
            return null;
          });
    }
    expectThrows(RuntimeException.class, () -> taskExecutor.invokeAll(callables));
    assertEquals(tasksWithNormalExit, tasksExecuted.get());
  }

  /**
   * Ensures that all invokeAll catches all exceptions thrown by Callables and adds subsequent ones
   * as suppressed exceptions to the first one caught.
   */
  public void testInvokeAllCatchesMultipleExceptions() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    List<Callable<Void>> callables = new ArrayList<>();
    callables.add(
        () -> {
          throw new RuntimeException("exception A");
        });
    int tasksWithNormalExit = 50;
    for (int i = 0; i < tasksWithNormalExit; i++) {
      callables.add(
          () -> {
            tasksExecuted.incrementAndGet();
            return null;
          });
    }
    callables.add(
        () -> {
          throw new IllegalStateException("exception B");
        });

    RuntimeException exc =
        expectThrows(RuntimeException.class, () -> taskExecutor.invokeAll(callables));
    Throwable[] suppressed = exc.getSuppressed();
    assertEquals(1, suppressed.length);
    if (exc.getMessage().equals("exception A")) {
      assertEquals("exception B", suppressed[0].getMessage());
    } else {
      assertEquals("exception A", suppressed[0].getMessage());
      assertEquals("exception B", exc.getMessage());
    }

    assertEquals(tasksWithNormalExit, tasksExecuted.get());
  }
}
