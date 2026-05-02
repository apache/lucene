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
package org.apache.lucene.tests.util;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import com.carrotsearch.randomizedtesting.jupiter.DetectThreadLeaks;
import com.carrotsearch.randomizedtesting.jupiter.SysProps;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.SuppressForbidden;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.testkit.engine.EngineTestKit;

/// groups all tests of the junit5/ jupiter parent class and its infrastructure.
@Execution(value = ExecutionMode.SAME_THREAD, reason = "single-threaded.")
public class TestLuceneTestCaseJupiter {
  /**
   * A test class that calls the provided callable at a specific pointcut (callback method, test).
   * This is used to make unit tests more compact.
   */
  static class CallAtPointcut extends LuceneTestCaseJupiter {
    enum Pointcut {
      BEFORE_ALL,
      BEFORE_EACH,
      TEST_NORMAL,
      TEST_DYNAMIC,
      TEST_PARAMETERIZED,
      AFTER_EACH,
      AFTER_ALL,
    }

    @SuppressWarnings("NonFinalStaticField")
    static Map<Pointcut, Executable> pointcuts;

    private static void call(Pointcut pointcut) throws Throwable {
      if (pointcuts.containsKey(pointcut)) {
        pointcuts.get(pointcut).execute();
      }
    }

    @BeforeAll
    static void beforeAll() throws Throwable {
      call(Pointcut.BEFORE_ALL);
    }

    @BeforeEach
    void beforeEach() throws Throwable {
      call(Pointcut.BEFORE_EACH);
    }

    @Test
    void normalTestMethod() throws Throwable {
      call(Pointcut.TEST_NORMAL);
    }

    @TestFactory
    Stream<DynamicTest> dynamicTests() throws Throwable {
      return Stream.of(
          DynamicTest.dynamicTest(
              "dynamicTest",
              () -> {
                call(Pointcut.TEST_DYNAMIC);
              }));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a"})
    void templateTest(String unused) throws Throwable {
      call(Pointcut.TEST_PARAMETERIZED);
    }

    @AfterEach
    void afterEach() throws Throwable {
      call(Pointcut.AFTER_EACH);
    }

    @AfterAll
    static void afterAll() throws Throwable {
      call(Pointcut.AFTER_ALL);
    }
  }

  /// Test cases should use parameter-injected [java.util.Random] or a supplier
  /// of [java.util.Random]. Avoid using static methods.
  @Nested
  class RandomInjection {
    @Test
    public void testRandomParameterInjected() {
      testKitBuilder(T1.class)
          .configurationParameter(SysProps.TESTS_SEED.propertyKey, "dead:beef:cafe")
          .execute()
          .allEvents()
          .assertThatEvents()
          .doNotHave(event(finishedWithFailure()));
    }

    static class T1 extends LuceneTestCaseJupiter {
      public T1(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @BeforeAll
      static void beforeAll(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @BeforeEach
      void beforeEach(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @Test
      void testMethod(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @Test
      void testMethodWithSupplier(Supplier<Random> supplier) {
        Assert.assertNotNull(supplier);
        Assert.assertNotNull(supplier.get());
      }

      @AfterEach
      void afterEach(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @AfterAll
      static void afterAll(Random rnd) {
        Assert.assertNotNull(rnd);
      }
    }
  }

  /// Verifies that [LuceneTestCaseJupiter.SuiteFailureTracker] correctly tracks test failures
  /// across various lifecycle methods of a [LuceneTestCaseJupiter] subclass.
  @Nested
  class SuiteFailureTracking {
    private static final List<Thread> forkedThreads = new ArrayList<>();

    @AfterEach
    void interruptAndJoinForkedThreads() throws InterruptedException {
      for (var t : forkedThreads) t.interrupt();
      for (var t : forkedThreads) t.join();
      forkedThreads.clear();
    }

    @SuppressForbidden(reason = "Thread sleep")
    private static void startSleepingThread(Duration duration) {
      startThread(
          "sleeping-thread",
          () -> {
            try {
              Thread.sleep(duration.toMillis());
            } catch (InterruptedException _) {
            }
          });
    }

    private static Thread startThread(String name, Runnable r) {
      var t = new Thread(r, name);
      forkedThreads.add(t);
      t.start();
      return t;
    }

    @Test
    public void testNoFailureLeavesMarkerClear() {
      testKitBuilder(SuccessfulSuite.class)
          .execute()
          .allEvents()
          .assertThatEvents()
          .doNotHave(event(finishedWithFailure()));
      Assert.assertTrue(LuceneTestCaseJupiter.suiteFailureTracker.wasSuccessful());
    }

    static class SuccessfulSuite extends LuceneTestCaseJupiter {
      @Test
      void testMethod() {}
    }

    @TestFactory
    Stream<DynamicTest> suiteFailureIsTracked() {
      return Stream.of(CallAtPointcut.Pointcut.values())
          .map(
              pointcut ->
                  DynamicTest.dynamicTest(
                      pointcut.name(),
                      () -> {
                        CallAtPointcut.pointcuts = Map.of(pointcut, Assertions::fail);
                        testKitBuilder(CallAtPointcut.class)
                            .execute()
                            .allEvents()
                            .assertThatEvents()
                            .haveAtLeast(1, event(finishedWithFailure()));
                        Assert.assertFalse(
                            LuceneTestCaseJupiter.suiteFailureTracker.wasSuccessful());
                      }));
    }

    @TestFactory
    Stream<DynamicTest> failedAssumptionsAreNotFailures() {
      return Stream.of(CallAtPointcut.Pointcut.values())
          .<Map.Entry<CallAtPointcut.Pointcut, Executable>>mapMulti(
              (pointcut, downstream) -> {
                downstream.accept(
                    Map.entry(
                        pointcut,
                        () -> {
                          org.junit.jupiter.api.Assumptions.assumeTrue(false);
                        }));
                downstream.accept(
                    Map.entry(
                        pointcut,
                        () -> {
                          org.assertj.core.api.Assumptions.assumeThat(true).isFalse();
                        }));
                downstream.accept(
                    Map.entry(
                        pointcut,
                        () -> {
                          Assume.assumeTrue(false);
                        }));
              })
          .map(
              entry -> {
                return DynamicTest.dynamicTest(
                    entry.getKey().name(),
                    () -> {
                      CallAtPointcut.pointcuts = Map.ofEntries(entry);
                      testKitBuilder(CallAtPointcut.class).execute();
                      Assert.assertTrue(LuceneTestCaseJupiter.suiteFailureTracker.wasSuccessful());
                    });
              });
    }

    @Test
    public void testFailBecauseOfLeakedThreads() {
      testKitBuilder(FailBecauseOfLeakedThreads.class)
          .execute()
          .allEvents()
          .assertThatEvents()
          .haveAtLeast(1, event(finishedWithFailure()));
      Assert.assertFalse(LuceneTestCaseJupiter.suiteFailureTracker.wasSuccessful());
    }

    @DetectThreadLeaks.LingerTime(millis = 0)
    static class FailBecauseOfLeakedThreads extends LuceneTestCaseJupiter {
      @Test
      void testMethod() {
        startSleepingThread(Duration.ofSeconds(10));
      }
    }
  }

  /// Verifies that failing to close a [org.apache.lucene.store.Directory] created during
  /// a test causes a test failure, mirroring the behavior of [TestFailIfDirectoryNotClosed]
  @Nested
  class UnclosedDirectoryTracking {
    @Test
    void testFailIfDirectoryNotClosed() {
      testKitBuilder(LeakyDirectorySuite.class)
          .execute()
          .allEvents()
          .assertThatEvents()
          .haveAtLeast(
              1,
              event(
                  finishedWithFailure(
                      instanceOf(AssertionError.class),
                      message(msg -> msg.contains(AssertDirectoryClosed.MSG_PREFIX)))));
    }

    static class LeakyDirectorySuite extends LuceneTestCaseJupiter {
      @Test
      void testLeaksDirectory() {
        LuceneTestCaseParent.newDirectory();
      }
    }
  }

  @Nested
  class ReproduceLinePrinter {
    @TestFactory
    Stream<DynamicTest> testReproLineIsPrinted() throws Exception {
      return Stream.of(CallAtPointcut.Pointcut.values())
          .map(
              pointcut -> {
                return DynamicTest.dynamicTest(
                    pointcut.name(),
                    () -> {
                      String testOutput = collectOutputFrom(Map.of(pointcut, Assertions::fail));

                      Assertions.assertThat(testOutput)
                          .contains(
                              LuceneTestCaseJupiter.PrintReproduceInfoExtension.TEST_REPRO_LEAD)
                          .contains(
                              LuceneTestCaseJupiter.PrintReproduceInfoExtension.TEST_ENV_LEAD);
                    });
              });
    }

    @TestFactory
    Stream<DynamicTest> testReproLineIsNotPrintedForAssumptions() throws Exception {
      return Stream.of(CallAtPointcut.Pointcut.values())
          .map(
              pointcut -> {
                return DynamicTest.dynamicTest(
                    pointcut.name(),
                    () -> {
                      String testOutput =
                          collectOutputFrom(
                              Map.of(
                                  pointcut,
                                  () -> {
                                    Assumptions.assumeTrue(false);
                                  }));

                      Assertions.assertThat(testOutput)
                          .doesNotContain(
                              LuceneTestCaseJupiter.PrintReproduceInfoExtension.TEST_REPRO_LEAD)
                          .doesNotContain(
                              LuceneTestCaseJupiter.PrintReproduceInfoExtension.TEST_ENV_LEAD);
                    });
              });
    }

    private static String collectOutputFrom(Map<CallAtPointcut.Pointcut, Executable> callables)
        throws IOException {
      String testOutput;
      try (var baos = new ByteArrayOutputStream();
          var pw = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
        CallAtPointcut.pointcuts = callables;
        LuceneTestCaseJupiter.PrintReproduceInfoExtension.debugStream = pw;
        testKitBuilder(CallAtPointcut.class).execute();
        pw.flush();
        testOutput = baos.toString(StandardCharsets.UTF_8);
      } finally {
        LuceneTestCaseJupiter.PrintReproduceInfoExtension.debugStream = null;
        CallAtPointcut.pointcuts = null;
      }
      return testOutput;
    }
  }

  @Nested
  class BackCompatBehavior {
    @Test
    public void testIndexWriterIsRestoredAfterEachTest() throws Exception {
      try {
        AlterIndexSearcher.expectedValue = IndexSearcher.getMaxClauseCount();
        testKitBuilder(AlterIndexSearcher.class)
            .execute()
            .allEvents()
            .assertThatEvents()
            .doNotHave(event(finishedWithFailure()));
      } finally {
        IndexSearcher.setMaxClauseCount(AlterIndexSearcher.expectedValue);
      }
    }

    static class AlterIndexSearcher extends LuceneTestCaseJupiter {
      @SuppressWarnings("NonFinalStaticField")
      static int expectedValue;

      @Test
      public void t1() {
        Assertions.assertThat(IndexSearcher.getMaxClauseCount()).isEqualTo(expectedValue);
        IndexSearcher.setMaxClauseCount(expectedValue + 1);
      }

      @Test
      public void t2() {
        Assertions.assertThat(IndexSearcher.getMaxClauseCount()).isEqualTo(expectedValue);
        IndexSearcher.setMaxClauseCount(expectedValue + 1);
      }
    }
  }

  public static EngineTestKit.Builder testKitBuilder(Class<?> testClass) {
    return testKitBuilder().selectors(selectClass(testClass));
  }

  public static EngineTestKit.Builder testKitBuilder() {
    return EngineTestKit.engine("junit-jupiter");
  }
}
