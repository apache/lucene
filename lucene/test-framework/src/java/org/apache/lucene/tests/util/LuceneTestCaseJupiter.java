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

import com.carrotsearch.randomizedtesting.jupiter.DetectThreadLeaks;
import com.carrotsearch.randomizedtesting.jupiter.Randomized;
import com.carrotsearch.randomizedtesting.jupiter.SystemThreadFilter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.util.Constants;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.commons.support.ReflectionSupport;

/*
TODOs.

- pick a smaller module and move (some?) of the tests to jupiter. Ensure both frameworks can coexist (jupiter and
vintage engine running both).
- add tests of the LuceneTestCaseJupiter infrastructure (if what was previously implemented
as rules in LuceneTestCase still provides the same functionality). Nested classes should be perhaps
excluded from discovery entirely (unless they're really needed/loaded?).
- resign from using GlobalStateSupport and move it to this class itself?
- add a check ensuring junit jupiter runs in single-thread mode (unfortunately this can't be
changed, at least for now).
- add all remaining class and test rules from LuceneTestCase; this is now the minimum subset.
 */

/// Base class for all Lucene unit tests (JUnit5/ Jupiter variant).
///
/// ## Class and instance setup
///
/// The preferred way to specify class (suite-level) setup/cleanup is to use static methods
/// annotated with [org.junit.jupiter.api.BeforeAll] and [org.junit.jupiter.api.AfterAll].
/// **Do not use static initializers (including complex final field initializers).**
///
/// For instance-level setup, use [BeforeEach] and
/// [AfterEach] annotated methods.
///
/// ## Specifying test cases
///
/// Any method of specifying JUnit jupiter tests will work. The most common way would therefore be:
/// ```java
/// @Test
/// public void testMethod(Random random) {}
/// ```
///
/// Note the (optional) [Random] argument - this is automatically populated for each test.
///
/// ## Randomized execution and test facilities
///
/// [LuceneTestCaseJupiter] uses the [Randomized] extension to support component randomization.
/// A [Random] can be automatically injected in the test (or any junit5 callback) as a parameter.
/// Tests should be fully reproducible for the same initial seed
/// (assuming no race conditions between threads
/// etc.). The initial seed for a test case is reported in many ways:
///
///   - logged from the gradle build,
///   - inserted as a synthetic stack frame in any exceptions.
///
/*
// TODO: port these.
- reproduce info listener, failuremarker? @Listeners({RunListenerPrintReproduceInfo.class, FailureMarker.class})
- predictable test ordering
- test sysout rule
@TestRuleLimitSysouts.Limit(
    bytes = TestRuleLimitSysouts.DEFAULT_LIMIT,
    hardLimit = TestRuleLimitSysouts.DEFAULT_HARD_LIMIT)
 */
@Randomized
@DetectThreadLeaks(scope = DetectThreadLeaks.Scope.SUITE)
@DetectThreadLeaks.LingerTime(millis = 20_000)
@DetectThreadLeaks.ExcludeThreads({
  SystemThreadFilter.class,
  LuceneTestCaseJupiter.IsSystemThread.class
})
@Timeout(value = 2, unit = TimeUnit.HOURS)
@Execution(
    value = ExecutionMode.SAME_THREAD,
    reason = "single-threaded for backward compatibility.")
@ExtendWith({GlobalStateSupport.class})
public abstract non-sealed class LuceneTestCaseJupiter extends LuceneTestCaseParent {
  /**
   * This predicate should return {@code true} for threads that should be ignored in {@linkplain
   * DetectThreadLeaks thread leak detection}.
   */
  public static class IsSystemThread implements Predicate<Thread> {
    static final boolean isJ9;

    static {
      isJ9 = Constants.JAVA_VENDOR.startsWith("IBM");
    }

    @Override
    public boolean test(Thread t) {
      var threadName = t.getName();
      switch (threadName) {
        case "ClassCache Reaper": // LUCENE-6518
        case "junit-jupiter-timeout-watcher": // junit5/jupiter timeouts.
        case "JNA Cleaner": // JNA cleaner thread (system).
          return true;
      }

      if (isJ9
          && Stream.of(t.getStackTrace())
              .anyMatch(frame -> frame.getClassName().equals("java.util.Timer$TimerImpl"))) {
        return true;
      }

      return false;
    }
  }

  // TODO: this should be in use (marker when tests failed).
  static TestRuleMarkFailure failureMarker = new TestRuleMarkFailure();

  @RegisterExtension
  static Extension failureListener =
      new TestWatcher() {
        @Override
        public void testAborted(ExtensionContext context, @Nullable Throwable cause) {
          failureMarker.markFailed();
        }

        @Override
        public void testFailed(ExtensionContext context, @Nullable Throwable cause) {
          failureMarker.markFailed();
        }
      };

  @BeforeAll
  static void setupClass(TestInfo testInfo) throws Throwable {
    var newRule =
        new TemporaryFilesSupplier(
            failureMarker,
            LuceneTestCaseJupiter::random,
            () -> testInfo.getTestClass().orElseThrow());
    failureMarker.reset();
    newRule.before();
  }

  @AfterAll
  static void teardownClass() throws Throwable {}

  //
  // Custom assertion methods.
  //

  public static int atLeast(Random random, int i) {
    return LuceneTestCase.atLeast(random, i);
  }

  //
  // Deprecated or removed methods (LuceneTestCase) and other backward-compatibility
  // infrastructure.
  //

  /**
   * Whenever possible, you should use an injected {@link Random} or {@code Supplier<Random>}
   * parameter on junit5 test methods.
   *
   * <p>Global static methods make running test suites in parallel impossible.
   */
  @Deprecated
  public static Random random() {
    return LuceneTestCaseParent.random();
  }

  /**
   * Unfortunately there is no easy way to implement custom test providers in jupiter so we just
   * enforce annotations on {@code test*} methods (so that they're not silently ignored).
   *
   * <p>A dynamic test factory would <em>almost</em> work but dynamic tests skip all the
   * before-after hooks so they're not a direct substitute.
   */
  @Test
  public void allTestMethodsAreAnnotated() {
    var testMethodsWithoutAnnotations =
        ReflectionSupport.findMethods(
            getClass(),
            m -> {
              return m.getName().startsWith("test")
                  && !ModifierSupport.isStatic(m)
                  && !AnnotationSupport.isAnnotated(m, Test.class);
            },
            HierarchyTraversalMode.BOTTOM_UP);

    if (!testMethodsWithoutAnnotations.isEmpty()) {
      throw new AssertionError(
          "test* methods must be annotated with @Test in junit5/jupiter, the following are not: "
              + testMethodsWithoutAnnotations.stream()
                  .map(m -> "\n  - " + m.getDeclaringClass().getName() + "#" + m.getName())
                  .collect(Collectors.joining()));
    }
  }

  /** Use methods marked with jupiter's {@link BeforeEach} instead. */
  @Override
  @BeforeEach
  protected final void setUp() throws Exception {
    super.setUp();
  }

  /** Use methods marked with jupiter's {@link AfterEach} instead. */
  @Override
  @AfterEach
  protected final void tearDown() throws Exception {
    super.tearDown();
  }

  public static <T extends Throwable> T expectThrows(
      Class<T> expectedType, LuceneTestCase.ThrowingRunnable runnable) {
    return LuceneTestCase.expectThrows(expectedType, runnable);
  }
}
