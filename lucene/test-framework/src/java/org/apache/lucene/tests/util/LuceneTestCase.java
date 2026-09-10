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

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsInt;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup.Group;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Tag;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

/// Base class for all Lucene unit tests (JUnit4 variant).
///
/// **Please consider using the new JUnit Jupiter base class [LuceneTestCaseJupiter] instead. This
/// class will be eventually removed from Lucene codebase.**
///
/// ## Class and instance setup
///
/// The preferred way to specify class (suite-level) setup/cleanup is to use static methods
/// annotated with [org.junit.BeforeClass] and [org.junit.AfterClass]. Any code in these methods
/// is executed
/// within the test framework's control and ensure proper setup has been made. **Try not to use
/// static initializers (including complex final field initializers).** Static initializers are
/// executed before any setup rules are fired and may cause you (or somebody else) headaches.
///
/// For instance-level setup, use [Before] and [After] annotated methods. If you
/// override either [#setUp()] or [#tearDown()] in your subclass, make sure you call
/// `super.setUp()` and `super.tearDown()`. This is detected and enforced.
///
/// ## Specifying test cases
///
/// Any test method with a `testXXX` prefix is considered a test case. Any test method
/// annotated with [org.junit.Test] is considered a test case. For example, these are equivalent
/// declarations:
///
/// ```java
/// public void testPrefixIsSufficient() {}
///
/// @Test
/// public void annotationIsRequiredHere() {}
/// ```
///
/// ## Randomized execution and test facilities
///
/// [LuceneTestCase] uses [RandomizedRunner] to execute test cases.
/// [RandomizedRunner] has built-in support for tests randomization including access to a repeatable
/// [Random] instance. See [#random()] method. Any test using [Random] acquired
/// from [#random()] should be fully reproducible (assuming no race conditions between threads
/// etc.). The initial seed for a test case is reported in many ways:
///
///   - as part of any exception thrown from its body (inserted as a dummy stack trace entry),
///   - as part of the main thread executing the test case (if your test hangs, just dump the stack
///     trace of all threads, and you'll see the seed),
///   - the master seed can also be accessed manually by getting the current context (
///     [RandomizedContext#current()]) and then callingd
///     [RandomizedContext#getRunnerSeedAsString()].
///
@RunWith(RandomizedRunner.class)
@TestMethodProviders({LuceneJUnit3MethodProvider.class, JUnit4MethodProvider.class})
@Listeners({RunListenerPrintReproduceInfo.class, FailureMarker.class})
@SeedDecorators({MixWithSuiteName.class}) // See LUCENE-3995 for rationale.
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakGroup(Group.MAIN)
@ThreadLeakAction({Action.WARN, Action.INTERRUPT})
// Wait long for leaked threads to complete before failure. zk needs this.
@ThreadLeakLingering(linger = 20000)
@ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
@TimeoutSuite(millis = 2 * TimeUnits.HOUR)
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {QuickPatchThreadsFilter.class})
@TestRuleLimitSysouts.Limit(
    bytes = TestRuleLimitSysouts.DEFAULT_LIMIT,
    hardLimit = TestRuleLimitSysouts.DEFAULT_HARD_LIMIT)
public abstract non-sealed class LuceneTestCase extends LuceneTestCaseParent {
  static final void ensureInitialized() {}

  /** Annotation for tests that should only be run during nightly builds. */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_NIGHTLY)
  @Tag("nightly")
  @TagState(enabled = false, sysProperty = SYSPROP_NIGHTLY)
  public @interface Nightly {}

  /** Annotation for tests that should only be run during weekly builds */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_WEEKLY)
  @Tag("weekly")
  @TagState(enabled = false, sysProperty = SYSPROP_WEEKLY)
  public @interface Weekly {}

  /** Annotation for monster tests that require special setup (e.g. use tons of disk and RAM) */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_MONSTER)
  @Tag("monster")
  @TagState(enabled = false, sysProperty = SYSPROP_MONSTER)
  public @interface Monster {
    String value();
  }

  /** Annotation for tests which exhibit a known issue and are temporarily disabled. */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_AWAITSFIX)
  @Tag("awaitsfix")
  @TagState(enabled = false, sysProperty = SYSPROP_AWAITSFIX)
  public @interface AwaitsFix {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  /**
   * Annotation for test classes that should avoid certain codec types (because they are expensive,
   * for example).
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressCodecs {
    String[] value();
  }

  /**
   * Annotation for test classes that should avoid mock filesystem types (because they test a bug
   * that only happens on linux, for example).
   *
   * <p>You can avoid specific names {@link Class#getSimpleName()} or use the special value <code>*
   * </code> to disable all mock filesystems.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressFileSystems {
    String[] value();
  }

  /**
   * Annotation for test classes that should avoid specific asserting formats within the {@link
   * AssertingCodec} while keeping other asserting formats active.
   *
   * @see org.apache.lucene.tests.codecs.asserting.AssertingCodec.Format
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressAssertingFormats {
    AssertingCodec.Format[] value();
  }

  /**
   * Annotation for test classes that should avoid always omit actual fsync calls from reaching the
   * filesystem.
   *
   * <p>This can be useful, e.g. if they make many lucene commits.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressFsync {}

  /**
   * Marks any suites which are known not to close all the temporary files. This may prevent temp.
   * files and folders from being cleaned up after the suite is completed.
   *
   * @see LuceneTestCase#createTempDir()
   * @see LuceneTestCase#createTempFile(String, String)
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressTempFileChecks {
    /** Point to JIRA entry. */
    public String bugUrl() default "None";
  }

  /**
   * Ignore {@link TestRuleLimitSysouts} for any suite which is known to print over the default
   * limit of bytes to {@link System#out} or {@link System#err}.
   *
   * @see TestRuleLimitSysouts
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressSysoutChecks {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  /**
   * Suppress the default {@code reproduce with: ant test...} Your own listener can be added as
   * needed for your build.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressReproduceLine {}

  // -----------------------------------------------------------------
  // Fields initialized in class or instance rules.
  // -----------------------------------------------------------------

  // -----------------------------------------------------------------
  // Class level (suite) rules.
  // -----------------------------------------------------------------

  /** Stores the current class under test. */
  private static final TestRuleStoreClassName classNameRule;

  /**
   * Ignore tests after hitting a designated number of initial failures. This is truly a "static"
   * global singleton since it needs to span the lifetime of all test classes running inside this
   * JVM (it cannot be part of a class rule).
   *
   * <p>This poses some problems for the test framework's tests because these sometimes trigger
   * intentional failures which add up to the global count. This field contains a (possibly)
   * changing reference to {@link TestRuleIgnoreAfterMaxFailures} and we dispatch to its current
   * value from the {@link #classRules} chain using {@link TestRuleDelegate}.
   */
  private static final AtomicReference<TestRuleIgnoreAfterMaxFailures>
      ignoreAfterMaxFailuresDelegate;

  private static final TestRule ignoreAfterMaxFailures;

  static {
    int maxFailures = systemPropertyAsInt(SYSPROP_MAXFAILURES, Integer.MAX_VALUE);
    boolean failFast = systemPropertyAsBoolean(SYSPROP_FAILFAST, false);

    if (failFast) {
      if (maxFailures == Integer.MAX_VALUE) {
        maxFailures = 1;
      } else {
        System.err.println(
            "Property '"
                + SYSPROP_MAXFAILURES
                + "'="
                + maxFailures
                + ", 'failfast' is"
                + " ignored.");
      }
    }

    ignoreAfterMaxFailuresDelegate =
        new AtomicReference<>(new TestRuleIgnoreAfterMaxFailures(maxFailures));
    ignoreAfterMaxFailures = TestRuleDelegate.of(ignoreAfterMaxFailuresDelegate);
  }

  /*
   * Try to capture streams early so that other classes don't have a chance to steal references to
   * them (as is the case with ju.logging handlers).
   */
  static {
    TestRuleLimitSysouts.checkCaptureStreams();
  }

  /**
   * Temporarily substitute the global {@link TestRuleIgnoreAfterMaxFailures}. See {@link
   * #ignoreAfterMaxFailuresDelegate} for some explanation why this method is needed.
   */
  public static TestRuleIgnoreAfterMaxFailures replaceMaxFailureRule(
      TestRuleIgnoreAfterMaxFailures newValue) {
    return ignoreAfterMaxFailuresDelegate.getAndSet(newValue);
  }

  /**
   * This controls how suite-level rules are nested. It is important that _all_ rules declared in
   * {@link LuceneTestCase} are executed in proper order if they depend on each other.
   */
  @ClassRule public static final TestRule classRules;

  @SuppressWarnings("NonFinalStaticField")
  private static TestRuleMarkFailure suiteFailureMarker;

  private static final FieldToType fieldToType;

  static {
    var setupAndRestoreClassEnv =
        new SetupAndRestoreStaticEnv(
            () -> RandomizedContext.current().getRandom(),
            () -> RandomizedContext.current().getTargetClass());

    suiteFailureMarker = new TestRuleMarkFailure();

    var tempFilesSupplier =
        new TemporaryFilesSupplier(
            suiteFailureMarker,
            LuceneTestCase::random,
            () -> RandomizedContext.current().getTargetClass());

    classRules =
        RuleChain.outerRule(new TestRuleIgnoreTestSuites())
            .around(
                new TestRuleAdapter() {
                  @SuppressWarnings("NonFinalStaticField")
                  private static TestFrameworkInfra testFrameworkInfra;

                  @Override
                  protected void before() throws Throwable {
                    int maxCalls =
                        Integer.parseInt(System.getProperty(SYSPROP_RANDOM_MAXCALLS, "0"));
                    Supplier<Random> supplier = () -> RandomizedContext.current().getRandom();
                    if (maxCalls > 0) {
                      var finalizedSupplier = supplier;
                      supplier = () -> new MaxCallCountRandom(finalizedSupplier.get(), maxCalls);
                    }

                    int maxAcquires =
                        Integer.parseInt(System.getProperty(SYSPROP_RANDOM_MAXACQUIRES, "0"));
                    if (maxAcquires > 0) {
                      var finalizedSupplier = supplier;
                      supplier =
                          () -> {
                            if (randomCalls.incrementAndGet() > maxAcquires) {
                              throw new RuntimeException(
                                  "Too many random() calls. Consider using LuceneTestCase.nonAssertingRandom for"
                                      + " large loops or data generation.");
                            }
                            return finalizedSupplier.get();
                          };
                    }

                    var finalizedSupplier = supplier;
                    setTestFrameworkInfra(
                        null,
                        testFrameworkInfra =
                            new TestFrameworkInfra() {
                              @Override
                              public Random threadRandom() {
                                return finalizedSupplier.get();
                              }

                              @Override
                              public SetupAndRestoreStaticEnv getClassEnv() {
                                return setupAndRestoreClassEnv;
                              }

                              @Override
                              public TemporaryFilesSupplier getTempFilesSupplier() {
                                return tempFilesSupplier;
                              }

                              @Override
                              public SuiteFailureState getSuiteFailureState() {
                                return suiteFailureMarker;
                              }

                              @Override
                              public Field newField(
                                  Random random, String name, Object value, FieldType type) {
                                return fieldToType.newField(random, name, value, type);
                              }
                            });
                  }

                  @Override
                  protected void afterAlways(List<Throwable> errors) {
                    setTestFrameworkInfra(testFrameworkInfra, null);
                  }
                })
            .around(ignoreAfterMaxFailures)
            .around(suiteFailureMarker)
            .around(
                new VerifyTestClassNamingConvention(
                    "org.apache.lucene", Pattern.compile("(.+\\.)(Test)([^.]+)")))
            .around(new TestRuleAssertionsRequired())
            .around(new TestRuleLimitSysouts(suiteFailureMarker))
            .around(new CallbacksToRuleAdapter(tempFilesSupplier))
            .around(new NoClassHooksShadowingRule())
            .around(
                new NoInstanceHooksOverridesRule() {
                  @Override
                  protected boolean verify(Method key) {
                    String name = key.getName();
                    return !(name.equals("setUp") || name.equals("tearDown"));
                  }
                })
            .around(classNameRule = new TestRuleStoreClassName())
            .around(
                new TestRuleRestoreSystemProperties(
                    // Enlist all properties to which we have write access (security manager);
                    // these should be restored to previous state, no matter what the outcome of the
                    // test.

                    // We reset the default locale and timezone; these properties change as a
                    // side-effect
                    "user.language", "user.timezone"))
            .around(new CallbacksToRuleAdapter(setupAndRestoreClassEnv))
            .around(new CallbacksToRuleAdapter(fieldToType = new FieldToType()))
            .around(
                new CallbacksToRuleAdapter(
                    new BeforeAfterCallback() {
                      @Override
                      public void before() {
                        // Save environment information to reproduce-info listener.
                        // This listener can be invoked after all the tests and other callbacks have
                        // completed; I don't see any clean way to pass it there.
                        RunListenerPrintReproduceInfo.envInfoJunit4 =
                            new TestEnvInfo(
                                setupAndRestoreClassEnv.codec,
                                setupAndRestoreClassEnv.similarity,
                                setupAndRestoreClassEnv.locale,
                                setupAndRestoreClassEnv.timeZone);
                      }
                    }));
  }

  // -----------------------------------------------------------------
  // Test level rules.
  // -----------------------------------------------------------------

  /** Enforces {@link #setUp()} and {@link #tearDown()} calls are chained. */
  private final TestRuleSetupTeardownChained parentChainCallRule =
      new TestRuleSetupTeardownChained();

  /** Save test thread and name. */
  private final TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

  /** Taint suite result with individual test failures. */
  private final TestRuleMarkFailure testFailureMarker = new TestRuleMarkFailure(suiteFailureMarker);

  /**
   * This controls how individual test rules are nested. It is important that _all_ rules declared
   * in {@link LuceneTestCase} are executed in proper order if they depend on each other.
   */
  @Rule
  public final TestRule ruleChain =
      RuleChain.outerRule(testFailureMarker)
          .around(ignoreAfterMaxFailures)
          .around(threadAndTestNameRule)
          .around(new CallbacksToRuleAdapter(new TestRuleSetupAndRestoreInstanceEnv()))
          .around(parentChainCallRule);

  /** A counter of calls to {@link #random()} if {@link #SYSPROP_RANDOM_MAXACQUIRES} is defined. */
  @SuppressWarnings("NonFinalStaticField")
  private static AtomicLong randomCalls = new AtomicLong();

  // -----------------------------------------------------------------
  // Suite and test case setup/ cleanup.
  // -----------------------------------------------------------------

  /** For subclasses to override. Overrides must call {@code super.setUp()}. */
  @Before
  public void setUp() throws Exception {
    randomCalls.set(0);
    parentChainCallRule.setupCalled = true;
  }

  /** For subclasses to override. Overrides must call {@code super.tearDown()}. */
  @After
  public void tearDown() throws Exception {
    parentChainCallRule.teardownCalled = true;

    // Test is supposed to call this itself, but we do this defensively in case it forgot:
    restoreIndexWriterMaxDocs();

    fieldToType.reset();
  }

  // -----------------------------------------------------------------
  // Test facilities and facades for subclasses.
  // -----------------------------------------------------------------

  /** Return the current class being tested. */
  public static Class<?> getTestClass() {
    return classNameRule.getTestClass();
  }

  /** Return the name of the currently executing test case. */
  public String getTestName() {
    return threadAndTestNameRule.testMethodName;
  }

  /**
   * Returns true if and only if the calling thread is the primary thread executing the test case.
   */
  protected boolean isTestThread() {
    assertNotNull("Test case thread not set?", threadAndTestNameRule.testCaseThread);
    return Thread.currentThread() == threadAndTestNameRule.testCaseThread;
  }
}
