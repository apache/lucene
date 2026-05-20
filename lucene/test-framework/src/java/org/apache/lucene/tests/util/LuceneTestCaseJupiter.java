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
import com.carrotsearch.randomizedtesting.jupiter.RandomizedContext;
import com.carrotsearch.randomizedtesting.jupiter.SystemThreadFilter;
import java.io.Closeable;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.util.IOUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ModifierSupport;
import org.junit.platform.commons.support.ReflectionSupport;
import org.opentest4j.TestAbortedException;

/// Base class for all Lucene unit tests (JUnit5/ Jupiter variant).
///
/// ## Class and instance setup
///
/// The preferred way to specify class (suite-level) setup/cleanup is to use static methods
/// annotated with [org.junit.jupiter.api.BeforeAll] and [org.junit.jupiter.api.AfterAll].
/// **Do not use static initializers (including complex final field initializers).**
///
/// For instance-level setup, use [org.junit.jupiter.api.BeforeEach] and
/// [org.junit.jupiter.api.AfterEach] annotated methods.
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
TODO: port the remaining infrastructure bits from LuceneTestCase:
- there are class rules and test rules that are still only on junit4 side
TestRuleRestoreSystemProperties
TestRuleLimitSysouts
NoInstanceHooksOverridesRule (?)
ignoreAfterMaxFailures
*/
@Randomized
@DetectThreadLeaks(scope = DetectThreadLeaks.Scope.SUITE)
@DetectThreadLeaks.LingerTime(millis = 20_000)
@DetectThreadLeaks.ExcludeThreads({SystemThreadFilter.class, IsSystemThread.class})
@Timeout(value = 2, unit = TimeUnit.HOURS)
@Execution(
    value = ExecutionMode.SAME_THREAD,
    reason = "single-threaded for backward compatibility.")
@ExtendWith(EnsureSequentialExecution.class)
@TestMethodOrder(CustomMethodOrderer.class)
public abstract non-sealed class LuceneTestCaseJupiter extends LuceneTestCaseParent {

  static final class PerThreadRandom implements BeforeAfterCallback {
    private final ConcurrentHashMap<Thread, Random> perThreadRandoms = new ConcurrentHashMap<>();
    private final Supplier<Random> supplier;

    PerThreadRandom(Supplier<Random> randomSupplier) {
      this.supplier = randomSupplier;
    }

    @Override
    public void after() throws Exception {
      IOUtils.close(
          perThreadRandoms.values().stream()
              .filter(v -> v instanceof Closeable)
              .map(v -> (Closeable) v)
              .toList());
    }

    Random get() {
      return perThreadRandoms.computeIfAbsent(Thread.currentThread(), _ -> supplier.get());
    }
  }

  static final class OrderedBeforeAfterCallbacks implements BeforeAfterCallback {
    final List<BeforeAfterCallback> callbacks;
    final ArrayDeque<BeforeAfterCallback> executed = new ArrayDeque<>();

    OrderedBeforeAfterCallbacks(List<BeforeAfterCallback> callbacks) {
      this.callbacks = callbacks;
    }

    @Override
    public void before() throws Exception {
      assert executed.isEmpty();
      for (var c : callbacks) {
        c.before();
        executed.addLast(c);
      }
    }

    @Override
    public void after() throws Exception {
      Throwable t = null;
      while (!executed.isEmpty()) {
        var c = executed.removeLast();
        try {
          c.after();
        } catch (Throwable ex) {
          if (t == null) {
            t = ex;
          } else {
            t.addSuppressed(ex);
          }
        }
      }

      if (t != null) {
        if (t instanceof Exception ex) {
          throw ex;
        } else if (t instanceof Error err) {
          throw err;
        } else /* only theoretically possible? */ {
          throw new RuntimeException(t);
        }
      }
    }
  }

  /// Tracks whether any test in the current suite had a failure.
  /// Registered before [ClassLevelCallbackChain] so its state is available during suite teardown.
  /// We plug into multiple jupiter extensions, hoping they will be sufficient to detect failure
  // state.
  static final class SuiteFailureTracker
      implements AfterAllCallback,
          BeforeAllCallback,
          TestWatcher,
          SuiteFailureState,
          InvocationInterceptor {
    private static final ExtensionContext.Namespace NAMESPACE =
        ExtensionContext.Namespace.create(SuiteFailureTracker.class);

    private volatile boolean hadFailures;

    @Override
    public void beforeAll(ExtensionContext context) {
      hadFailures = false;
      // Register a Closeable invoked after all the other callbacks have been called:
      // @BeforeAll/@AfterAll lifecycle methods and peer extension afterAll callbacks
      // (e.g. DetectThreadLeaks).
      context
          .getStore(NAMESPACE)
          .put("failureCheck", (AutoCloseable) () -> checkContextException(context));
    }

    @Override
    public void afterAll(ExtensionContext context) {
      checkContextException(context);
    }

    private void checkContextException(ExtensionContext context) {
      if (context.getExecutionException().isPresent()) {
        if (!isFailedAssumption(context.getExecutionException().get())) {
          hadFailures = true;
        }
      }
    }

    public static boolean isFailedAssumption(Throwable t) {
      return t instanceof TestAbortedException
          || t.getClass().getName().equals("org.junit.AssumptionViolatedException");
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
      hadFailures = true;
    }

    @Override
    public void testAborted(ExtensionContext context, @Nullable Throwable cause) {
      // This means the test threw an assumption exception. Ignored.
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
      // This means the test is ignored for some other reason.
    }

    // required to detect failures in dynamic tests.
    @Override
    public void interceptDynamicTest(
        Invocation<@Nullable Void> invocation,
        DynamicTestInvocationContext invocationContext,
        ExtensionContext extensionContext)
        throws Throwable {
      try {
        invocation.proceed();
      } catch (Throwable t) {
        if (!isFailedAssumption(t)) {
          hadFailures = true;
        }
        throw t;
      }
    }

    @Override
    public boolean wasSuccessful() {
      return !hadFailures;
    }
  }

  /// This extension sets up junit-jupiter implementations of the
  /// test framework-dependent infrastructure in [LuceneTestCaseParent].
  ///
  /// It tries to simulate before-after rules as they are implemented in junit4
  /// (call order, unwinding in case of failures, etc.).
  static class ClassLevelCallbackChain
      implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {
    private final SuiteFailureTracker suiteFailureTracker;
    private final PrintReproduceInfoExtension printReproduceInfo;

    private TestFrameworkInfra jupiterFrameworkInfra;
    private OrderedBeforeAfterCallbacks beforeAfters;

    private FieldToType fieldToType;

    ClassLevelCallbackChain(
        SuiteFailureTracker suiteFailureTracker,
        PrintReproduceInfoExtension printReproduceInfoExtension) {
      this.suiteFailureTracker = suiteFailureTracker;
      this.printReproduceInfo = printReproduceInfoExtension;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
      // touch LuceneTestCase to trigger static initializers.
      LuceneTestCase.ensureInitialized();

      var activeTestClass = context.getRequiredTestClass();

      // these hooks need to run in the right order of before-after calls,
      // with the expected nesting.
      var classEnvRule =
          new SetupAndRestoreStaticEnv(LuceneTestCaseJupiter::random, () -> activeTestClass);
      var tempFileSupplier =
          new TemporaryFilesSupplier(
              suiteFailureTracker, LuceneTestCaseJupiter::random, () -> activeTestClass);
      var perThreadRandom = new PerThreadRandom(getRandomSupplier(context.getExecutableInvoker()));

      fieldToType = new FieldToType();

      this.jupiterFrameworkInfra =
          new TestFrameworkInfra() {
            @Override
            public Random threadRandom() {
              return perThreadRandom.get();
            }

            @Override
            public SetupAndRestoreStaticEnv getClassEnv() {
              return classEnvRule;
            }

            @Override
            public TemporaryFilesSupplier getTempFilesSupplier() {
              return tempFileSupplier;
            }

            @Override
            public SuiteFailureState getSuiteFailureState() {
              return suiteFailureTracker;
            }

            @Override
            public Field newField(Random random, String name, Object value, FieldType type) {
              return fieldToType.newField(random, name, value, type);
            }
          };

      var installFrameworkInfraSupport =
          new BeforeAfterCallback() {
            @Override
            public void before() {
              LuceneTestCaseParent.setTestFrameworkInfra(null, jupiterFrameworkInfra);
            }

            @Override
            public void after() {
              LuceneTestCaseParent.setTestFrameworkInfra(jupiterFrameworkInfra, null);
            }
          };

      var installEnvInfo =
          new BeforeAfterCallback() {
            @Override
            public void before() throws Exception {
              printReproduceInfo.testEnvInfo =
                  new TestEnvInfo(
                      classEnvRule.codec,
                      classEnvRule.similarity,
                      classEnvRule.locale,
                      classEnvRule.timeZone);
            }
          };

      // This is the chain of before-after callbacks that must be called in the right order for
      // compatibility
      // with junit4 implementation.
      this.beforeAfters =
          new OrderedBeforeAfterCallbacks(
              List.of(
                  perThreadRandom,
                  installFrameworkInfraSupport,
                  classEnvRule,
                  fieldToType,
                  installEnvInfo,
                  tempFileSupplier));

      beforeAfters.before();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      fieldToType.after();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
      beforeAfters.after();
    }

    // This trick is needed to get the Supplier<Random> injected by a parameter resolved
    // of the randomized testing framework.
    private Supplier<Random> getRandomSupplier(ExecutableInvoker executableInvoker)
        throws Exception {
      var hack =
          new Object() {
            @SuppressWarnings("unused")
            public Supplier<Random> captureParameter(Supplier<Random> rnd) {
              return rnd;
            }
          };

      @SuppressWarnings("unchecked")
      Supplier<Random> rnd =
          (Supplier<Random>)
              Objects.requireNonNull(
                  executableInvoker.invoke(
                      hack.getClass().getMethod("captureParameter", Supplier.class), hack));
      return rnd;
    }
  }

  static class PrintReproduceInfoExtension
      implements BeforeAllCallback, TestWatcher, InvocationInterceptor {
    public static final CharSequence TEST_REPRO_LEAD = TestEnvInfo.TEST_REPRO_LEAD;
    public static final CharSequence TEST_ENV_LEAD = TestEnvInfo.TEST_ENV_LEAD;

    // Used for tests only to replace syserrs.
    @SuppressWarnings("NonFinalStaticField")
    public static PrintStream debugStream;

    TestEnvInfo testEnvInfo;

    private String rootSeed;
    private boolean somethingFailed;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
      this.rootSeed = getRandomSupplier(context.getExecutableInvoker()).getRootSeed().toString();

      // Register a Closeable invoked after all the other callbacks have been called:
      // @BeforeAll/@AfterAll lifecycle methods and peer extension afterAll callbacks
      // (e.g. DetectThreadLeaks).
      context
          .getStore(ExtensionContext.Namespace.create(PrintReproduceInfoExtension.class))
          .put(
              "failureCheck",
              (AutoCloseable)
                  () -> {
                    if (context.getExecutionException().isPresent()) {
                      if (!SuiteFailureTracker.isFailedAssumption(
                          context.getExecutionException().get())) {
                        printReproduceInfo(context);
                        somethingFailed = true;
                      }
                    }

                    if (somethingFailed && testEnvInfo != null) {
                      println(testEnvInfo.getDebuggingInformation());
                    }
                  });
    }

    @Override
    public void testFailed(ExtensionContext context, @Nullable Throwable cause) {
      somethingFailed = true;
      printReproduceInfo(context);
    }

    // Required to detect failures in dynamic tests.
    @Override
    public void interceptDynamicTest(
        Invocation<@Nullable Void> invocation,
        DynamicTestInvocationContext invocationContext,
        ExtensionContext extensionContext)
        throws Throwable {
      try {
        invocation.proceed();
      } catch (Throwable t) {
        if (!SuiteFailureTracker.isFailedAssumption(t)) {
          somethingFailed = true;
          printReproduceInfo(extensionContext);
        }
        throw t;
      }
    }

    private void printReproduceInfo(ExtensionContext context) {
      if (testEnvInfo == null || rootSeed == null) {
        println(
            "NOTE: test failed but no environment information is present to construct the reproduce-line.");
      } else {
        println(
            testEnvInfo.getAdditionalFailureInfo(
                rootSeed,
                b -> {
                  // TODO: add gradle infrastructure to rerun tests based on their uniqueid
                  // instead of their class/method. This would allow dynamic tests to be
                  // repeatable.

                  if (context.getTestClass().isPresent()) {
                    b.append("--tests ");
                    b.append(context.getRequiredTestClass().getName());
                    if (context.getTestMethod().isPresent()) {
                      b.append(".").append(context.getRequiredTestMethod().getName());
                    }
                  }
                }));
      }
    }

    private void println(String msg) {
      if (debugStream != null) {
        debugStream.println(msg);
      } else {
        System.err.println(msg);
      }
    }

    // This trick is needed to get the Supplier<Random> injected by a parameter resolved
    // of the randomized testing framework.
    private RandomizedContext getRandomSupplier(ExecutableInvoker executableInvoker)
        throws Exception {
      var hack =
          new Object() {
            @SuppressWarnings("unused")
            public RandomizedContext captureParameter(RandomizedContext ctx) {
              return ctx;
            }
          };

      return (RandomizedContext)
          Objects.requireNonNull(
              executableInvoker.invoke(
                  hack.getClass().getMethod("captureParameter", RandomizedContext.class), hack));
    }
  }

  static class TestLevelCallbackChain implements BeforeEachCallback, AfterEachCallback {
    private OrderedBeforeAfterCallbacks callbacks;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      this.callbacks =
          new OrderedBeforeAfterCallbacks(List.of(new TestRuleSetupAndRestoreInstanceEnv()));
      callbacks.before();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      callbacks.after();
    }
  }

  @RegisterExtension
  @Order(0)
  static final SuiteFailureTracker suiteFailureTracker = new SuiteFailureTracker();

  @RegisterExtension
  @Order(1)
  static final PrintReproduceInfoExtension printReproduceInfoExtension =
      new PrintReproduceInfoExtension();

  @RegisterExtension
  @Order(2)
  static final ClassLevelCallbackChain classLevelCallbackChain =
      new ClassLevelCallbackChain(
          Objects.requireNonNull(suiteFailureTracker),
          Objects.requireNonNull(printReproduceInfoExtension));

  @RegisterExtension
  @Order(3)
  static final TestLevelCallbackChain testLevelCallbackChain = new TestLevelCallbackChain();

  //
  // Deprecated or removed methods (LuceneTestCase) and other backward-compatibility
  // infrastructure.
  //

  /// [BeforeEach] isn't inherited in junit5, so call this method explicitly, even if overridden.
  @BeforeEach
  void callSetUp() throws Exception {
    setUp();
  }

  /// a [BeforeEach] callback. In subclasses, if you override, you must call `super.setUp` too.
  public void setUp() throws Exception {}

  /// [AfterEach] isn't inherited in junit5, so call this method explicitly, even if overridden.
  @AfterEach
  public void callTearDown() throws Exception {
    tearDown();
  }

  /// a [AfterEach] callback. In subclasses, if you override, you must call `super.setUp` too.
  public void tearDown() throws Exception {}

  /**
   * Use explicit, injected {@link Random} or {@code Supplier<Random>} parameters on junit jupiter
   * test methods (or callbacks).
   */
  @Deprecated
  public static Random random() {
    return LuceneTestCaseParent.random();
  }

  @Test
  void verifyTestAssertionStatus() throws Exception {
    TestRuleAssertionsRequired.checkAssertionStatus();
  }

  /** Enforce test class naming convention. */
  @Test
  void enforceClassNamingConvention(TestInfo testInfo) {
    new VerifyTestClassNamingConvention(
            "org.apache.lucene", Pattern.compile("(.+\\.)(Test)([^.]+)"))
        .check(testInfo.getTestClass().orElseThrow());
  }

  /**
   * Unfortunately there is no easy way to implement custom test providers in jupiter so we just
   * enforce annotations on {@code test*} methods (so that they're not silently ignored).
   *
   * <p>A dynamic test factory would <em>almost</em> work but dynamic tests skip all the
   * before-after hooks so they're not a direct substitute.
   */
  @Test
  void allTestMethodsAreAnnotated(TestInfo testInfo) {
    var testMethodsWithoutAnnotations =
        ReflectionSupport.findMethods(
            testInfo.getTestClass().orElseThrow(),
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
}
