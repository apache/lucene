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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
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

  public void testInvokeAllFromTaskDoesNotDeadlockSameSearcher() throws Exception {
    doTestInvokeAllFromTaskDoesNotDeadlockSameSearcher(executorService);
    doTestInvokeAllFromTaskDoesNotDeadlockSameSearcher(Runnable::run);
    executorService
        .submit(
            () -> {
              doTestInvokeAllFromTaskDoesNotDeadlockSameSearcher(executorService);
              return null;
            })
        .get();
  }

  private static void doTestInvokeAllFromTaskDoesNotDeadlockSameSearcher(Executor executor)
      throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 500; i++) {
        iw.addDocument(new Document());
      }
      try (DirectoryReader reader = iw.getReader()) {
        IndexSearcher searcher =
            new IndexSearcher(reader, executor) {
              @Override
              protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                return slices(leaves, 1, 1, false);
              }
            };

        searcher.search(
            MatchAllDocsQuery.INSTANCE,
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

  public void testInvokeAllFromTaskDoesNotDeadlockMultipleSearchers() throws Exception {
    doTestInvokeAllFromTaskDoesNotDeadlockMultipleSearchers(executorService);
    doTestInvokeAllFromTaskDoesNotDeadlockMultipleSearchers(Runnable::run);
    executorService
        .submit(
            () -> {
              doTestInvokeAllFromTaskDoesNotDeadlockMultipleSearchers(executorService);
              return null;
            })
        .get();
  }

  private static void doTestInvokeAllFromTaskDoesNotDeadlockMultipleSearchers(Executor executor)
      throws IOException {
    try (Directory dir = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
      for (int i = 0; i < 500; i++) {
        iw.addDocument(new Document());
      }
      try (DirectoryReader reader = iw.getReader()) {
        IndexSearcher searcher =
            new IndexSearcher(reader, executor) {
              @Override
              protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                return slices(leaves, 1, 1, false);
              }
            };

        searcher.search(
            MatchAllDocsQuery.INSTANCE,
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
                        IndexSearcher indexSearcher = new IndexSearcher(reader, executor);
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
    AtomicInteger tasksStarted = new AtomicInteger(0);
    TaskExecutor taskExecutor =
        new TaskExecutor(
            command -> {
              tasksStarted.incrementAndGet();
              command.run();
            });
    AtomicInteger tasksExecuted = new AtomicInteger(0);
    List<Callable<Void>> callables = new ArrayList<>();
    callables.add(
        () -> {
          tasksExecuted.incrementAndGet();
          throw new RuntimeException();
        });
    int tasksWithNormalExit = 99;
    for (int i = 0; i < tasksWithNormalExit; i++) {
      callables.add(
          () -> {
            throw new AssertionError(
                "must not be called since the first task failing cancels all subsequent tasks");
          });
    }
    expectThrows(RuntimeException.class, () -> taskExecutor.invokeAll(callables));
    assertEquals(1, tasksExecuted.get());
    // the callables are technically all run, but the cancelled ones will be no-op
    assertEquals(tasksWithNormalExit, tasksStarted.get());
  }

  /**
   * Ensures that all invokeAll catches all exceptions thrown by Callables and adds subsequent ones
   * as suppressed exceptions to the first one caught.
   */
  public void testInvokeAllCatchesMultipleExceptions() {
    // this test requires multiple threads, while all the other tests in this class rely on a single
    // threaded executor
    ExecutorService multiThreadedExecutor =
        Executors.newFixedThreadPool(
            2, new NamedThreadFactory(TestTaskExecutor.class.getSimpleName()));
    try {
      TaskExecutor taskExecutor = new TaskExecutor(multiThreadedExecutor);
      List<Callable<Void>> callables = new ArrayList<>();
      // if we have multiple threads, make sure both are started before an exception is thrown,
      // otherwise there may or may not be a suppressed exception
      CountDownLatch latchA = new CountDownLatch(1);
      CountDownLatch latchB = new CountDownLatch(1);
      callables.add(
          () -> {
            latchA.countDown();
            latchB.await();
            throw new RuntimeException("exception A");
          });
      callables.add(
          () -> {
            latchB.countDown();
            latchA.await();
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
    } finally {
      TestUtil.shutdownExecutorService(multiThreadedExecutor);
    }
  }

  public void testCancelTasksOnException() {
    TaskExecutor taskExecutor = new TaskExecutor(Runnable::run);
    final int numTasks = random().nextInt(10, 50);
    final int throwingTask = random().nextInt(numTasks);
    boolean error = random().nextBoolean();
    List<Callable<Void>> tasks = new ArrayList<>(numTasks);
    AtomicInteger executedTasks = new AtomicInteger(0);
    for (int i = 0; i < numTasks; i++) {
      final int index = i;
      tasks.add(
          () -> {
            if (index == throwingTask) {
              if (error) {
                throw new OutOfMemoryError();
              } else {
                throw new RuntimeException();
              }
            }
            if (index > throwingTask) {
              // with a single thread we are sure that the last task to run is the one that throws,
              // following ones must not run
              throw new AssertionError("task should not have started");
            }
            executedTasks.incrementAndGet();
            return null;
          });
    }
    Throwable throwable;
    if (error) {
      throwable = expectThrows(OutOfMemoryError.class, () -> taskExecutor.invokeAll(tasks));
    } else {
      throwable = expectThrows(RuntimeException.class, () -> taskExecutor.invokeAll(tasks));
    }
    assertEquals(0, throwable.getSuppressed().length);
    assertEquals(throwingTask, executedTasks.get());
  }

  public void testTaskRejectionDoesNotFailExecution() throws Exception {
    try (ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new NamedThreadFactory("TestTaskExecutor"))) {
      final int taskCount = 1000; // enough tasks to cause queuing and rejections on the executor
      final ArrayList<Callable<Void>> callables = new ArrayList<>(taskCount);
      final AtomicInteger executedTasks = new AtomicInteger(0);
      for (int i = 0; i < taskCount; i++) {
        callables.add(
            () -> {
              executedTasks.incrementAndGet();
              return null;
            });
      }
      final TaskExecutor taskExecutor = new TaskExecutor(threadPoolExecutor);
      var res = taskExecutor.invokeAll(callables);
      assertEquals(taskCount, res.size());
      assertEquals(taskCount, executedTasks.get());
    }
  }

  public void testNoTaskGroupOutsideOfTasks() {
    assertSame(TaskExecutor.TaskGroup.NONE, TaskExecutor.currentTaskGroup());
    assertFalse(TaskExecutor.TaskGroup.NONE.hasSiblings());
  }

  public void testTaskWithoutSiblings() throws IOException {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    TaskExecutor.TaskGroup group =
        taskExecutor
            .invokeAll(List.<Callable<TaskExecutor.TaskGroup>>of(TaskExecutor::currentTaskGroup))
            .getFirst();
    assertNotSame(TaskExecutor.TaskGroup.NONE, group);
    assertFalse(group.hasSiblings());
  }

  public void testSiblingTasksShareTaskGroup() throws IOException {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    List<Callable<TaskExecutor.TaskGroup>> tasks = new ArrayList<>();
    int numTasks = random().nextInt(2, 10);
    for (int i = 0; i < numTasks; i++) {
      tasks.add(TaskExecutor::currentTaskGroup);
    }
    List<TaskExecutor.TaskGroup> groups = taskExecutor.invokeAll(tasks);
    assertTrue(groups.getFirst().hasSiblings());
    for (TaskExecutor.TaskGroup group : groups) {
      assertSame(groups.getFirst(), group);
    }
    assertFalse(groups.getFirst().isFailed());
    assertNotSame(groups.getFirst(), taskExecutor.invokeAll(tasks).getFirst());
    assertSame(TaskExecutor.TaskGroup.NONE, TaskExecutor.currentTaskGroup());
  }

  public void testRunningTasksExitEarlyOnFailure() {
    try (ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("TestTaskExecutor"))) {
      TaskExecutor taskExecutor = new TaskExecutor(executor);
      CountDownLatch running = new CountDownLatch(1);
      boolean error = random().nextBoolean();
      List<Callable<Void>> tasks = new ArrayList<>();
      tasks.add(
          () -> {
            running.countDown();
            awaitTaskGroupFailure(TaskExecutor.currentTaskGroup()).ensureNotFailed();
            throw new AssertionError("unreachable");
          });
      tasks.add(
          () -> {
            assertTrue(running.await(30, TimeUnit.SECONDS));
            if (error) {
              throw new OutOfMemoryError("boom");
            }
            throw new IOException("boom");
          });
      Collections.shuffle(tasks, random());
      Class<? extends Throwable> expected = error ? OutOfMemoryError.class : IOException.class;
      Throwable t = expectThrows(expected, () -> taskExecutor.invokeAll(tasks));
      assertEquals(0, t.getSuppressed().length);
    }
  }

  public void testCancellationExceptionIsAFailureIfNoOtherTaskFailed() {
    TaskExecutor taskExecutor = new TaskExecutor(executorService);
    List<Callable<Void>> tasks = new ArrayList<>();
    tasks.add(() -> null);
    tasks.add(
        () -> {
          throw new CancellationException("boom");
        });
    Collections.shuffle(tasks, random());
    CancellationException e =
        expectThrows(CancellationException.class, () -> taskExecutor.invokeAll(tasks));
    assertEquals("boom", e.getMessage());
  }

  public void testTaskGroupsWithVirtualThreads() throws Exception {
    final int numGroups = random().nextInt(2, 8);
    final int tasksPerGroup = random().nextInt(2, 8);
    // all tasks of all groups are alive at the same time and unmount while waiting, so that carrier
    // threads run tasks of different groups: this fails if the group is stored per carrier thread
    CountDownLatch allRunning = new CountDownLatch(numGroups * tasksPerGroup);
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      TaskExecutor taskExecutor = new TaskExecutor(executor);
      List<Callable<TaskExecutor.TaskGroup>> tasks = new ArrayList<>();
      for (int i = 0; i < tasksPerGroup; i++) {
        tasks.add(
            () -> {
              TaskExecutor.TaskGroup group = TaskExecutor.currentTaskGroup();
              allRunning.countDown();
              assertTrue(allRunning.await(30, TimeUnit.SECONDS));
              Thread.yield();
              assertSame(group, TaskExecutor.currentTaskGroup());
              return group;
            });
      }
      List<Future<List<TaskExecutor.TaskGroup>>> invocations = new ArrayList<>();
      for (int i = 0; i < numGroups; i++) {
        invocations.add(executor.submit(() -> taskExecutor.invokeAll(tasks)));
      }
      Set<TaskExecutor.TaskGroup> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Future<List<TaskExecutor.TaskGroup>> invocation : invocations) {
        List<TaskExecutor.TaskGroup> groups = invocation.get();
        for (TaskExecutor.TaskGroup group : groups) {
          assertSame(groups.getFirst(), group);
        }
        distinct.add(groups.getFirst());
      }
      assertEquals(numGroups, distinct.size());
    }
  }

  public void testNestedTasksGetTheirOwnTaskGroup() throws Exception {
    try (ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("TestTaskExecutor"))) {
      TaskExecutor taskExecutor = new TaskExecutor(executor);
      Callable<Void> task =
          () -> {
            TaskExecutor.TaskGroup outer = TaskExecutor.currentTaskGroup();
            TaskExecutor.TaskGroup single =
                taskExecutor
                    .invokeAll(
                        List.<Callable<TaskExecutor.TaskGroup>>of(TaskExecutor::currentTaskGroup))
                    .getFirst();
            assertNotSame(outer, single);
            assertFalse(single.hasSiblings());
            List<TaskExecutor.TaskGroup> siblings =
                taskExecutor.invokeAll(
                    List.of(TaskExecutor::currentTaskGroup, TaskExecutor::currentTaskGroup));
            assertNotSame(outer, siblings.getFirst());
            assertSame(siblings.getFirst(), siblings.getLast());
            assertSame(outer, TaskExecutor.currentTaskGroup());
            return null;
          };
      taskExecutor.invokeAll(List.of(task, task));
    }
  }

  public void testExitingBecauseOfAnotherGroupIsAFailure() {
    try (ExecutorService executor =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("TestTaskExecutor"))) {
      TaskExecutor taskExecutor = new TaskExecutor(executor);
      CountDownLatch running = new CountDownLatch(1);
      List<Callable<Void>> tasks = new ArrayList<>();
      tasks.add(
          () -> {
            TaskExecutor.TaskGroup outer = TaskExecutor.currentTaskGroup();
            List<Callable<Void>> innerTasks = new ArrayList<>();
            innerTasks.add(
                () -> {
                  running.countDown();
                  // the inner group did not fail, so this must fail the inner group
                  awaitTaskGroupFailure(outer).ensureNotFailed();
                  throw new AssertionError("unreachable");
                });
            innerTasks.add(() -> null);
            expectThrows(CancellationException.class, () -> taskExecutor.invokeAll(innerTasks));
            outer.ensureNotFailed();
            throw new AssertionError("unreachable");
          });
      tasks.add(
          () -> {
            assertTrue(running.await(30, TimeUnit.SECONDS));
            throw new IOException("boom");
          });
      Collections.shuffle(tasks, random());
      IOException e = expectThrows(IOException.class, () -> taskExecutor.invokeAll(tasks));
      assertEquals("boom", e.getMessage());
      assertEquals(0, e.getSuppressed().length);
    }
  }

  /** Waits until another task of the given group fails. */
  static TaskExecutor.TaskGroup awaitTaskGroupFailure(TaskExecutor.TaskGroup group) {
    assertTrue(group.hasSiblings());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (group.isFailed() == false) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("the failure of a sibling task was not observed");
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    return group;
  }
}
