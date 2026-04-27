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
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.util.IOUtils;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
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
@DetectThreadLeaks.ExcludeThreads({SystemThreadFilter.class, IsSystemThread.class})
@Timeout(value = 2, unit = TimeUnit.HOURS)
@Execution(
    value = ExecutionMode.SAME_THREAD,
    reason = "single-threaded for backward compatibility.")
public abstract non-sealed class LuceneTestCaseJupiter extends LuceneTestCaseParent {
  private static final class JupiterTestFrameworkInfra
      implements LuceneTestCaseParent.TestFrameworkInfra {
    private final ConcurrentHashMap<Thread, Random> perThreadRandoms = new ConcurrentHashMap<>();

    private final List<Closeable> closeAfterTest = new ArrayList<>();
    private final List<Closeable> closeAfterSuite = new ArrayList<>();

    final Supplier<Random> rnd;

    private final TemporaryFilesSupplier tempFilesSupplier;
    private final SetupAndRestoreStaticEnv classEnvRule;

    JupiterTestFrameworkInfra(
        SetupAndRestoreStaticEnv classEnvRule,
        TemporaryFilesSupplier tempFileSupplier,
        Supplier<Random> randomSupplier) {
      this.classEnvRule = classEnvRule;
      this.rnd = randomSupplier;
      this.tempFilesSupplier = tempFileSupplier;
    }

    @Override
    public Random threadRandom() {
      return perThreadRandoms.computeIfAbsent(Thread.currentThread(), _ -> rnd.get());
    }

    @Override
    public <T extends Closeable> T closeAfterTest(T resource) {
      synchronized (this) {
        closeAfterTest.add(resource);
      }
      return resource;
    }

    @Override
    public <T extends Closeable> T closeAfterClass(T resource) {
      synchronized (this) {
        closeAfterSuite.add(resource);
      }
      return resource;
    }

    @Override
    public SetupAndRestoreStaticEnv getClassEnv() {
      return classEnvRule;
    }

    @Override
    public TemporaryFilesSupplier getTempFilesSupplier() {
      return tempFilesSupplier;
    }
  }

  static final class OrderedBeforeAfterCallback implements BeforeAfterCallback {
    final List<BeforeAfterCallback> callbacks;
    final ArrayDeque<BeforeAfterCallback> executed = new ArrayDeque<>();

    OrderedBeforeAfterCallback(List<BeforeAfterCallback> callbacks) {
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

  static class SetupTestFrameworkInfra implements BeforeAllCallback, AfterAllCallback {
    private JupiterTestFrameworkInfra jupiterFrameworkInfra;

    TestRuleMarkFailure failureMarker;
    SetupAndRestoreStaticEnv classEnvRule;
    TemporaryFilesSupplier tempFileSupplier;

    private OrderedBeforeAfterCallback beforeAfters;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
      // touch LuceneTestCase to trigger static initializers.
      LuceneTestCase.ensureInitialized();

      var activeTestClass = context.getRequiredTestClass();
      this.classEnvRule =
          new SetupAndRestoreStaticEnv(LuceneTestCaseJupiter::random, () -> activeTestClass);
      this.failureMarker = new TestRuleMarkFailure();
      this.tempFileSupplier =
          new TemporaryFilesSupplier(
              failureMarker, LuceneTestCaseJupiter::random, () -> activeTestClass);
      this.jupiterFrameworkInfra =
          new JupiterTestFrameworkInfra(
              classEnvRule, tempFileSupplier, getRandomSupplier(context.getExecutableInvoker()));

      var setFramework =
          new BeforeAfterCallback() {
            @Override
            public void before() {
              LuceneTestCaseParent.setTestFrameworkInfra(null, jupiterFrameworkInfra);
            }

            @Override
            public void after() throws Exception {
              IOUtils.close(
                  Stream.of(
                          jupiterFrameworkInfra.closeAfterTest,
                          jupiterFrameworkInfra.closeAfterSuite,
                          jupiterFrameworkInfra.perThreadRandoms.values())
                      .flatMap(Collection::stream)
                      .filter(v -> v instanceof Closeable)
                      .map(v -> (Closeable) v)
                      .toList());

              LuceneTestCaseParent.setTestFrameworkInfra(jupiterFrameworkInfra, null);
            }
          };

      this.beforeAfters =
          new OrderedBeforeAfterCallback(
              List.of(setFramework, classEnvRule, failureMarker, tempFileSupplier));
      beforeAfters.before();
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

  @RegisterExtension
  @Order(0)
  static SetupTestFrameworkInfra classCallbacks = new SetupTestFrameworkInfra();

  @RegisterExtension
  Extension testCallbackChain =
      new AfterEachCallback() {
        @Override
        public void afterEach(ExtensionContext context) throws Exception {
          fieldToType.after();
        }
      };

  //
  // Deprecated or removed methods (LuceneTestCase) and other backward-compatibility
  // infrastructure.
  //

  /**
   * Use explicit, injected {@link Random} or {@code Supplier<Random>} parameters on junit jupiter
   * test methods (or callbacks).
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
}
