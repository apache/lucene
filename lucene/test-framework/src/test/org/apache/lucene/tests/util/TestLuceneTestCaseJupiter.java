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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.lucene.util.SuppressForbidden;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.testkit.engine.EngineTestKit;

/// groups all tests of the junit5/ jupiter parent class and its infrastructure.
@Execution(value = ExecutionMode.SAME_THREAD, reason = "single-threaded.")
public class TestLuceneTestCaseJupiter {
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

    @TestFactory
    Stream<DynamicTest> suiteFailureIsTracked() {
      return Stream.of(
              FailInTestMethod.class,
              FailInBeforeEach.class,
              FailInAfterEach.class,
              FailInBeforeAll.class,
              FailInAfterAll.class,
              FailBecauseOfLeakedThreads.class)
          .map(
              clazz ->
                  DynamicTest.dynamicTest(
                      clazz.getSimpleName(),
                      () -> {
                        testKitBuilder(clazz)
                            .execute()
                            .allEvents()
                            .assertThatEvents()
                            .haveAtLeast(1, event(finishedWithFailure()));
                        Assert.assertFalse(
                            LuceneTestCaseJupiter.suiteFailureTracker.wasSuccessful());
                      }));
    }

    static class SuccessfulSuite extends LuceneTestCaseJupiter {
      @Test
      void testOk() {}
    }

    static class FailInTestMethod extends LuceneTestCaseJupiter {
      @Test
      void testFails() {
        throw new AssertionError();
      }
    }

    static class FailInBeforeEach extends LuceneTestCaseJupiter {
      @BeforeEach
      void beforeEach() {
        throw new AssertionError();
      }

      @Test
      void testMethod() {}
    }

    static class FailInAfterEach extends LuceneTestCaseJupiter {
      @AfterEach
      void afterEach() {
        throw new AssertionError();
      }

      @Test
      void testMethod() {}
    }

    static class FailInBeforeAll extends LuceneTestCaseJupiter {
      @BeforeAll
      static void setUpSuite() {
        throw new AssertionError();
      }

      @Test
      void testMethod() {}
    }

    static class FailInAfterAll extends LuceneTestCaseJupiter {
      @AfterAll
      static void tearDownSuite() {
        throw new AssertionError();
      }

      @Test
      void testMethod() {}
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

  public static EngineTestKit.Builder testKitBuilder(Class<?> testClass) {
    return testKitBuilder().selectors(selectClass(testClass));
  }

  public static EngineTestKit.Builder testKitBuilder() {
    return EngineTestKit.engine("junit-jupiter");
  }
}
