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
import com.carrotsearch.randomizedtesting.LifecycleScope;
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
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.bitvectors.HnswBitVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.ParallelCompositeReader;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.tests.index.AssertingLeafReader;
import org.apache.lucene.tests.index.FieldFilterLeafReader;
import org.apache.lucene.tests.index.MergingCodecReader;
import org.apache.lucene.tests.index.MergingDirectoryReaderWrapper;
import org.apache.lucene.tests.index.MismatchedCodecReader;
import org.apache.lucene.tests.index.MismatchedDirectoryReader;
import org.apache.lucene.tests.index.MismatchedLeafReader;
import org.apache.lucene.tests.mockfile.VirusCheckingFS;
import org.apache.lucene.tests.search.AssertingIndexSearcher;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.store.RawDirectoryWrapper;
import org.apache.lucene.util.CommandLineUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Tag;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

/// Base class for all Lucene unit tests (JUnit4 variant).
///
/// ## Class and instance setup
///
/// The preferred way to specify class (suite-level) setup/cleanup is to use static methods
/// annotated with [BeforeClass] and [AfterClass]. Any code in these methods is executed
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
///     [RandomizedContext#current()]) and then calling
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

  static {
    var setupAndRestoreClassEnv =
        new SetupAndRestoreStaticEnv(
            () -> RandomizedContext.current().getRandom(),
            () -> RandomizedContext.current().getTargetClass());

    classRules =
        RuleChain.outerRule(new TestRuleIgnoreTestSuites())
            .around(
                new TestRuleAdapter() {
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

                    int maxAquires =
                        Integer.parseInt(System.getProperty(SYSPROP_RANDOM_MAXACQUIRES, "0"));
                    if (maxAquires > 0) {
                      var finalizedSupplier = supplier;
                      supplier =
                          () -> {
                            if (randomCalls.incrementAndGet() > maxAquires) {
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
                              public <T extends Closeable> T closeAfterTest(T resource) {
                                return RandomizedContext.current()
                                    .closeAtEnd(resource, LifecycleScope.TEST);
                              }

                              @Override
                              public <T extends Closeable> T closeAfterClass(T resource) {
                                return RandomizedContext.current()
                                    .closeAtEnd(resource, LifecycleScope.SUITE);
                              }

                              @Override
                              public void afterAll() {}

                              @Override
                              public void afterEach() {}

                              @Override
                              public SetupAndRestoreStaticEnv getClassEnv() {
                                return setupAndRestoreClassEnv;
                              }
                            });
                  }

                  @Override
                  protected void afterAlways(List<Throwable> errors) {
                    setTestFrameworkInfra(testFrameworkInfra, null);
                  }
                })
            .around(ignoreAfterMaxFailures)
            .around(suiteFailureMarker = new TestRuleMarkFailure())
            .around(
                new VerifyTestClassNamingConvention(
                    "org.apache.lucene", Pattern.compile("(.+\\.)(Test)([^.]+)")))
            .around(new TestRuleAssertionsRequired())
            .around(new TestRuleLimitSysouts(suiteFailureMarker))
            .around(
                tempFilesCleanupRule =
                    new TestRuleTemporaryFilesCleanup(
                        suiteFailureMarker,
                        LuceneTestCase::random,
                        () -> RandomizedContext.current().getTargetClass()))
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
            .around(
                new CallbacksToRuleAdapter(
                    new BeforeAfterCallback() {
                      @Override
                      public void before() throws Exception {
                        RunListenerPrintReproduceInfo.env =
                            new RunListenerPrintReproduceInfo.Env(
                                setupAndRestoreClassEnv.codec,
                                setupAndRestoreClassEnv.similarity,
                                setupAndRestoreClassEnv.locale,
                                setupAndRestoreClassEnv.timeZone);
                      }

                      @Override
                      public void after() throws Exception {}
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
          .around(new TestRuleSetupAndRestoreInstanceEnv())
          .around(parentChainCallRule);

  /** A counter of calls to {@link #random()} if {@link #SYSPROP_RANDOM_MAXACQUIRES} is defined. */
  @SuppressWarnings("NonFinalStaticField")
  private static AtomicLong randomCalls = new AtomicLong();

  // -----------------------------------------------------------------
  // Suite and test case setup/ cleanup.
  // -----------------------------------------------------------------

  /** For subclasses to override. Overrides must call {@code super.setUp()}. */
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    randomCalls.set(0);
    parentChainCallRule.setupCalled = true;
  }

  /** For subclasses to override. Overrides must call {@code super.tearDown()}. */
  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    parentChainCallRule.teardownCalled = true;

    // Test is supposed to call this itself, but we do this defensively in case it forgot:
    restoreIndexWriterMaxDocs();
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

  /**
   * Returns a new Directory instance. Use this when the test does not care about the specific
   * Directory implementation (most tests).
   *
   * <p>The Directory is wrapped with {@link BaseDirectoryWrapper}. this means usually it will be
   * picky, such as ensuring that you properly close it and all open files in your test. It will
   * emulate some features of Windows, such as not allowing open files to be overwritten.
   */
  public static BaseDirectoryWrapper newDirectory() {
    return newDirectory(random());
  }

  /** Like {@link #newDirectory} except randomly the {@link VirusCheckingFS} may be installed */
  public static BaseDirectoryWrapper newMaybeVirusCheckingDirectory() {
    if (random().nextInt(5) == 4) {
      Path path = addVirusChecker(createTempDir());
      return newFSDirectory(path);
    } else {
      return newDirectory(random());
    }
  }

  /**
   * Returns a new Directory instance, using the specified random. See {@link #newDirectory()} for
   * more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r) {
    return wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), rarely(r), false);
  }

  /**
   * Returns a new Directory instance, using the specified random. See {@link #newDirectory()} for
   * more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r, LockFactory lf) {
    return wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY, lf), rarely(r), false);
  }

  public static MockDirectoryWrapper newMockDirectory() {
    return newMockDirectory(random());
  }

  public static MockDirectoryWrapper newMockDirectory(Random r) {
    return (MockDirectoryWrapper)
        wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), false, false);
  }

  public static MockDirectoryWrapper newMockDirectory(Random r, LockFactory lf) {
    return (MockDirectoryWrapper)
        wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY, lf), false, false);
  }

  public static MockDirectoryWrapper newMockFSDirectory(Path f) {
    return (MockDirectoryWrapper) newFSDirectory(f, FSLockFactory.getDefault(), false);
  }

  public static MockDirectoryWrapper newMockFSDirectory(Path f, LockFactory lf) {
    return (MockDirectoryWrapper) newFSDirectory(f, lf, false);
  }

  public static Path addVirusChecker(Path path) {
    if (TestUtil.hasVirusChecker(path) == false) {
      VirusCheckingFS fs = new VirusCheckingFS(path.getFileSystem(), random().nextLong());
      path = fs.wrapPath(path);
    }
    return path;
  }

  /**
   * Returns a new Directory instance, with contents copied from the provided directory. See {@link
   * #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Directory d) throws IOException {
    return newDirectory(random(), d);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(Path f) {
    return newFSDirectory(f, FSLockFactory.getDefault());
  }

  /** Like {@link #newFSDirectory(Path)}, but randomly insert {@link VirusCheckingFS} */
  public static BaseDirectoryWrapper newMaybeVirusCheckingFSDirectory(Path f) {
    if (random().nextInt(5) == 4) {
      f = addVirusChecker(f);
    }
    return newFSDirectory(f, FSLockFactory.getDefault());
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf) {
    return newFSDirectory(f, lf, rarely());
  }

  private static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf, boolean bare) {
    String fsdirClass = TEST_DIRECTORY;
    if (fsdirClass.equals("random")) {
      fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES);
    }

    Class<? extends FSDirectory> clazz;
    try {
      try {
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      } catch (ClassCastException _) {
        // TEST_DIRECTORY is not a sub-class of FSDirectory, so draw one at random
        fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES);
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      }

      Directory fsdir = newFSDirectoryImpl(clazz, f, lf);
      return wrapDirectory(random(), fsdir, bare, true);
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }

  private static Directory newFileSwitchDirectory(Random random, Directory dir1, Directory dir2) {
    List<String> fileExtensions =
        Arrays.asList(
            "fdt", "fdx", "tim", "tip", "si", "fnm", "pos", "dii", "dim", "nvm", "nvd", "dvm",
            "dvd");
    Collections.shuffle(fileExtensions, random);
    fileExtensions = fileExtensions.subList(0, 1 + random.nextInt(fileExtensions.size()));
    return new FileSwitchDirectory(new HashSet<>(fileExtensions), dir1, dir2, true);
  }

  /**
   * Returns a new Directory instance, using the specified random with contents copied from the
   * provided directory. See {@link #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    for (String file : d.listAll()) {
      if (file.startsWith(IndexFileNames.SEGMENTS)
          || IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
        impl.copyFrom(d, file, file, newIOContext(r));
      }
    }
    return wrapDirectory(r, impl, rarely(r), false);
  }

  private static BaseDirectoryWrapper wrapDirectory(
      Random random, Directory directory, boolean bare, boolean filesystem) {
    // IOContext randomization might make NRTCachingDirectory make bad decisions, so avoid
    // using it if the user requested a filesystem directory.
    if (rarely(random) && !bare && filesystem == false) {
      directory = new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
    }

    if (bare) {
      BaseDirectoryWrapper base = new RawDirectoryWrapper(directory);
      closeAfterSuite(new CloseableDirectory(base, suiteFailureMarker));
      return base;
    } else {
      MockDirectoryWrapper mock = new MockDirectoryWrapper(random, directory);

      mock.setThrottling(TEST_THROTTLING);
      closeAfterSuite(new CloseableDirectory(mock, suiteFailureMarker));
      return mock;
    }
  }

  private static Directory newFSDirectoryImpl(
      Class<? extends FSDirectory> clazz, Path path, LockFactory lf) throws IOException {
    FSDirectory d = null;
    try {
      d = CommandLineUtil.newFSDirectory(clazz, path, lf);
    } catch (ReflectiveOperationException e) {
      Rethrow.rethrow(e);
    }
    return d;
  }

  static Directory newDirectoryImpl(Random random, String clazzName) {
    return newDirectoryImpl(random, clazzName, FSLockFactory.getDefault());
  }

  static Directory newDirectoryImpl(Random random, String clazzName, LockFactory lf) {
    if (clazzName.equals("random")) {
      if (rarely(random)) {
        clazzName = RandomPicks.randomFrom(random, CORE_DIRECTORIES);
      } else if (rarely(random)) {
        String clazzName1 =
            rarely(random)
                ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
                : ByteBuffersDirectory.class.getName();
        String clazzName2 =
            rarely(random)
                ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
                : ByteBuffersDirectory.class.getName();
        return newFileSwitchDirectory(
            random,
            newDirectoryImpl(random, clazzName1, lf),
            newDirectoryImpl(random, clazzName2, lf));
      } else {
        clazzName = ByteBuffersDirectory.class.getName();
      }
    }

    try {
      final Class<? extends Directory> clazz = CommandLineUtil.loadDirectoryClass(clazzName);
      // If it is a FSDirectory type, try its ctor(Path)
      if (FSDirectory.class.isAssignableFrom(clazz)) {
        final Path dir = createTempDir("index-" + clazzName);
        return newFSDirectoryImpl(clazz.asSubclass(FSDirectory.class), dir, lf);
      }

      // See if it has a Path/LockFactory ctor even though it's not an
      // FSDir subclass:
      try {
        Constructor<? extends Directory> pathCtor =
            clazz.getConstructor(Path.class, LockFactory.class);
        final Path dir = createTempDir("index");
        return pathCtor.newInstance(dir, lf);
      } catch (NoSuchMethodException _) {
        // Ignore
      }

      // the remaining dirs are no longer filesystem based, so we must check that the
      // passedLockFactory is not file based:
      if (!(lf instanceof FSLockFactory)) {
        // try ctor with only LockFactory
        try {
          return clazz.getConstructor(LockFactory.class).newInstance(lf);
        } catch (NoSuchMethodException _) {
          // Ignore
        }
      }

      // try empty ctor
      return clazz.getConstructor().newInstance();
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }

  public static IndexReader wrapReader(IndexReader r) throws IOException {
    Random random = random();

    for (int i = 0, c = random.nextInt(6) + 1; i < c; i++) {
      switch (random.nextInt(5)) {
        case 0:
          // will create no FC insanity in atomic case, as ParallelLeafReader has own cache key:
          if (VERBOSE) {
            System.out.println(
                "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                    + r
                    + " with ParallelLeaf/CompositeReader");
          }
          r =
              (r instanceof LeafReader)
                  ? new ParallelLeafReader((LeafReader) r)
                  : new ParallelCompositeReader((CompositeReader) r);
          break;
        case 1:
          if (r instanceof LeafReader ar) {
            final List<String> allFields = new ArrayList<>();
            for (FieldInfo fi : ar.getFieldInfos()) {
              allFields.add(fi.name);
            }
            Collections.shuffle(allFields, random);
            final int end = allFields.isEmpty() ? 0 : random.nextInt(allFields.size());
            final Set<String> fields = new HashSet<>(allFields.subList(0, end));
            // will create no FC insanity as ParallelLeafReader has own cache key:
            if (VERBOSE) {
              System.out.println(
                  "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                      + r
                      + " with ParallelLeafReader");
            }
            r =
                new ParallelLeafReader(
                    new FieldFilterLeafReader(ar, fields, false),
                    new FieldFilterLeafReader(ar, fields, true));
          }
          break;
        case 2:
          // Häckidy-Hick-Hack: a standard Reader will cause FC insanity, so we use
          // QueryUtils' reader with a fake cache key, so insanity checker cannot walk
          // along our reader:
          if (VERBOSE) {
            System.out.println(
                "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                    + r
                    + " with AssertingLeaf/DirectoryReader");
          }
          if (r instanceof LeafReader) {
            r = new AssertingLeafReader((LeafReader) r);
          } else if (r instanceof DirectoryReader) {
            r = new AssertingDirectoryReader((DirectoryReader) r);
          }
          break;
        case 3:
          if (VERBOSE) {
            System.out.println(
                "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                    + r
                    + " with MismatchedLeaf/Directory/CodecReader");
          }
          if (r instanceof LeafReader) {
            r = new MismatchedLeafReader((LeafReader) r, random);
          } else if (r instanceof DirectoryReader) {
            r = new MismatchedDirectoryReader((DirectoryReader) r, random);
          } else if (r instanceof CodecReader) {
            r = new MismatchedCodecReader((CodecReader) r, random);
          }
          break;
        case 4:
          if (VERBOSE) {
            System.out.println(
                "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                    + r
                    + " with MergingCodecReader");
          }
          if (r instanceof CodecReader) {
            r = new MergingCodecReader((CodecReader) r);
          } else if (r instanceof DirectoryReader) {
            boolean allLeavesAreCodecReaders = true;
            for (LeafReaderContext ctx : r.leaves()) {
              if (ctx.reader() instanceof CodecReader == false) {
                allLeavesAreCodecReaders = false;
                break;
              }
            }
            if (allLeavesAreCodecReaders) {
              r = new MergingDirectoryReaderWrapper((DirectoryReader) r);
            }
          }
          break;
        default:
          fail("should not get here");
      }
    }

    if (VERBOSE) {
      System.out.println("wrapReader wrapped: " + r);
    }

    return r;
  }

  /** Sometimes wrap the IndexReader as slow, parallel or filter reader (or combinations of that) */
  public static IndexReader maybeWrapReader(IndexReader r) throws IOException {
    if (rarely()) {
      r = wrapReader(r);
    }
    return r;
  }

  /** TODO: javadoc */
  public static IOContext newIOContext(Random random) {
    return newIOContext(random, IOContext.DEFAULT);
  }

  /** TODO: javadoc */
  public static IOContext newIOContext(Random random, IOContext oldContext) {
    if (oldContext.hints().contains(ReadOnceHint.INSTANCE)) {
      return oldContext; // just return as-is
    }
    final int randomNumDocs = random.nextInt(4192);
    final int size = random.nextInt(512) * randomNumDocs;
    if (oldContext.flushInfo() != null) {
      // Always return at least the estimatedSegmentSize of
      // the incoming IOContext:
      return IOContext.flush(
          new FlushInfo(
              randomNumDocs, Math.max(oldContext.flushInfo().estimatedSegmentSize(), size)));
    } else if (oldContext.mergeInfo() != null) {
      // Always return at least the estimatedMergeBytes of
      // the incoming IOContext:
      return IOContext.merge(
          new MergeInfo(
              randomNumDocs,
              Math.max(oldContext.mergeInfo().estimatedMergeBytes(), size),
              random.nextBoolean(),
              TestUtil.nextInt(random, 1, 100)));
    } else {
      // Make a totally random IOContext
      final IOContext context;
      switch (random.nextInt(3)) {
        case 0:
          context = IOContext.DEFAULT;
          break;
        case 1:
          context = IOContext.merge(new MergeInfo(randomNumDocs, size, true, -1));
          break;
        case 2:
          context = IOContext.flush(new FlushInfo(randomNumDocs, size));
          break;
        default:
          context = IOContext.DEFAULT;
      }
      return context;
    }
  }

  private static final QueryCache DEFAULT_QUERY_CACHE = IndexSearcher.getDefaultQueryCache();
  private static final QueryCachingPolicy DEFAULT_CACHING_POLICY =
      IndexSearcher.getDefaultQueryCachingPolicy();
  private static final List<LRUQueryCache> queryCacheList = new ArrayList<>();

  @Before
  public void overrideTestDefaultQueryCache() {
    // Make sure each test method has its own cache
    overrideDefaultQueryCache();
  }

  @BeforeClass
  public static void overrideDefaultQueryCache() {
    // we need to reset the query cache in an @BeforeClass so that tests that
    // instantiate an IndexSearcher in an @BeforeClass method use a fresh new cache
    LRUQueryCache queryCacheTemp =
        new LRUQueryCache(10000, 1 << 25, _ -> true, Float.POSITIVE_INFINITY);
    queryCacheList.add(queryCacheTemp);
    IndexSearcher.setDefaultQueryCache(queryCacheTemp);
    IndexSearcher.setDefaultQueryCachingPolicy(MAYBE_CACHE_POLICY);
  }

  @AfterClass
  public static void resetDefaultQueryCache() {
    IndexSearcher.setDefaultQueryCache(DEFAULT_QUERY_CACHE);
    IndexSearcher.setDefaultQueryCachingPolicy(DEFAULT_CACHING_POLICY);
    for (int i = 0; i < queryCacheList.size(); i++) {
      try {
        queryCacheList.get(i).close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @BeforeClass
  public static void setupCPUCoreCount() {
    // Randomize core count so CMS varies its dynamic defaults, and this also "fixes" core
    // count from the master seed so it will always be the same on reproduce:
    int numCores = TestUtil.nextInt(random(), 1, 4);
    System.setProperty(
        ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY, Integer.toString(numCores));
  }

  @AfterClass
  public static void restoreCPUCoreCount() {
    System.clearProperty(ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY);
  }

  private static ExecutorService executor;

  @BeforeClass
  public static void setUpExecutorService() {
    int threads = TestUtil.nextInt(random(), 1, 2);
    executor =
        new ThreadPoolExecutor(
            threads,
            threads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory("LuceneTestCase"));
    // uncomment to intensify LUCENE-3840
    // executor.prestartAllCoreThreads();
    if (VERBOSE) {
      System.out.println("NOTE: Created shared ExecutorService with " + threads + " threads");
    }
  }

  @AfterClass
  public static void shutdownExecutorService() {
    TestUtil.shutdownExecutorService(executor);
    executor = null;
  }

  /** Create a new searcher over the reader. This searcher might randomly use threads. */
  public static IndexSearcher newSearcher(IndexReader r) {
    return newSearcher(r, true);
  }

  /** Create a new searcher over the reader. This searcher might randomly use threads. */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
    return newSearcher(r, maybeWrap, true);
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use threads. if <code>
   * maybeWrap</code> is true, this searcher might wrap the reader with one that returns null for
   * getSequentialSubReaders. If <code>wrapWithAssertions</code> is true, this searcher might be an
   * {@link AssertingIndexSearcher} instance.
   */
  public static IndexSearcher newSearcher(
      IndexReader r, boolean maybeWrap, boolean wrapWithAssertions) {
    return newSearcher(r, maybeWrap, wrapWithAssertions, random().nextBoolean());
  }

  /**
   * Create a new searcher over the reader. If <code>
   * maybeWrap</code> is true, this searcher might wrap the reader with one that returns null for
   * getSequentialSubReaders. If <code>wrapWithAssertions</code> is true, this searcher might be an
   * {@link AssertingIndexSearcher} instance. The searcher will use threads if <code>useThreads
   * </code> is set to true.
   */
  public static IndexSearcher newSearcher(
      IndexReader r, boolean maybeWrap, boolean wrapWithAssertions, boolean useThreads) {
    if (useThreads) {
      return newSearcher(r, maybeWrap, wrapWithAssertions, Concurrency.INTRA_SEGMENT);
    }
    return newSearcher(r, maybeWrap, wrapWithAssertions, Concurrency.NONE);
  }

  /** What level of concurrency is supported by the searcher being created */
  public enum Concurrency {
    /** No concurrency, meaning an executor won't be provided to the searcher */
    NONE,
    /**
     * Inter-segment concurrency, meaning an executor will be provided to the searcher and slices
     * will be randomly created to concurrently search entire segments
     */
    INTER_SEGMENT,
    /**
     * Intra-segment concurrency, meaning an executor will be provided to the searcher and slices
     * will be randomly created to concurrently search segment partitions
     */
    INTRA_SEGMENT
  }

  public static IndexSearcher newSearcher(
      IndexReader r, boolean maybeWrap, boolean wrapWithAssertions, Concurrency concurrency) {
    Random random = random();
    if (concurrency == Concurrency.NONE) {
      if (maybeWrap) {
        try {
          r = maybeWrapReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      // TODO: this whole check is a coverage hack, we should move it to tests for various
      // filterreaders.
      // ultimately whatever you do will be checkIndex'd at the end anyway.
      if (random.nextInt(500) == 0 && r instanceof LeafReader) {
        // TODO: not useful to check DirectoryReader (redundant with checkindex)
        // but maybe sometimes run this on the other crazy readers maybeWrapReader creates?
        try {
          TestUtil.checkReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      final IndexSearcher ret;
      if (wrapWithAssertions) {
        ret =
            random.nextBoolean()
                ? new AssertingIndexSearcher(random, r)
                : new AssertingIndexSearcher(random, r.getContext());
      } else {
        ret = random.nextBoolean() ? new IndexSearcher(r) : new IndexSearcher(r.getContext());
      }
      ret.setSimilarity(getTestFrameworkInfra().getClassEnv().similarity);
      return ret;
    } else {
      final ExecutorService ex;
      if (random.nextBoolean()) {
        ex = null;
      } else {
        ex = executor;
        if (VERBOSE) {
          System.out.println("NOTE: newSearcher using shared ExecutorService");
        }
      }
      IndexSearcher ret;
      int maxDocPerSlice = random.nextBoolean() ? 1 : 1 + random.nextInt(1000);
      int maxSegmentsPerSlice = random.nextBoolean() ? 1 : 1 + random.nextInt(10);
      if (wrapWithAssertions) {
        if (random.nextBoolean()) {
          ret =
              new AssertingIndexSearcher(random, r, ex) {
                @Override
                protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                  return LuceneTestCaseParent.slices(
                      leaves, maxDocPerSlice, maxSegmentsPerSlice, concurrency);
                }
              };
        } else {
          ret =
              new AssertingIndexSearcher(random, r.getContext(), ex) {
                @Override
                protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                  return LuceneTestCaseParent.slices(
                      leaves, maxDocPerSlice, maxSegmentsPerSlice, concurrency);
                }
              };
        }
      } else {
        ret =
            new IndexSearcher(r, ex) {
              @Override
              protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                return LuceneTestCaseParent.slices(
                    leaves, maxDocPerSlice, maxSegmentsPerSlice, concurrency);
              }
            };
      }
      ret.setSimilarity(getTestFrameworkInfra().getClassEnv().similarity);
      ret.setQueryCachingPolicy(MAYBE_CACHE_POLICY);
      if (random().nextBoolean()) {
        ret.setTimeout(() -> false);
      }
      return ret;
    }
  }

  /**
   * Creates an empty, temporary folder (when the name of the folder is of no importance).
   *
   * @see #createTempDir(String)
   */
  public static Path createTempDir() {
    return createTempDir("tempDir");
  }

  /**
   * Creates an empty, temporary folder with the given name prefix.
   *
   * <p>The folder will be automatically removed after the test class completes successfully. The
   * test should close any file handles that would prevent the folder from being removed.
   */
  public static Path createTempDir(String prefix) {
    return tempFilesCleanupRule.createTempDir(prefix);
  }

  /**
   * Creates an empty file with the given prefix and suffix.
   *
   * <p>The file will be automatically removed after the test class completes successfully. The test
   * should close any file handles that would prevent the folder from being removed.
   */
  public static Path createTempFile(String prefix, String suffix) throws IOException {
    return tempFilesCleanupRule.createTempFile(prefix, suffix);
  }

  /**
   * Creates an empty temporary file.
   *
   * @see #createTempFile(String, String)
   */
  public static Path createTempFile() throws IOException {
    return createTempFile("tempFile", ".tmp");
  }

  /** Ensures that the MergePolicy has sane values for tests that test with lots of documents. */
  protected static IndexWriterConfig ensureSaneIWCOnNightly(IndexWriterConfig conf) {
    if (LuceneTestCase.TEST_NIGHTLY) {
      // newIWConfig makes smallish max seg size, which
      // results in tons and tons of segments for this test
      // when run nightly:
      MergePolicy mp = conf.getMergePolicy();
      if (mp instanceof TieredMergePolicy) {
        ((TieredMergePolicy) mp).setMaxMergedSegmentMB(5000.);
      } else if (mp instanceof LogByteSizeMergePolicy) {
        ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1000.);
      } else if (mp instanceof LogMergePolicy) {
        ((LogMergePolicy) mp).setMaxMergeDocs(100000);
      }
      // when running nightly, merging can still have crazy parameters,
      // and might use many per-field codecs. turn on CFS for IW flushes
      // and ensure CFS ratio is reasonable to keep it contained.
      conf.setUseCompoundFile(true);
    }
    return conf;
  }

  private static boolean supportsVectorEncoding(
      KnnVectorsFormat format, VectorEncoding vectorEncoding) {
    if (format instanceof HnswBitVectorsFormat) {
      // special case, this only supports BYTE
      return vectorEncoding == VectorEncoding.BYTE;
    }
    return true;
  }

  private static boolean supportsVectorSearch(KnnVectorsFormat format) {
    return (format instanceof FlatVectorsFormat) == false;
  }

  protected static KnnVectorsFormat randomVectorFormat(VectorEncoding vectorEncoding) {
    List<KnnVectorsFormat> availableFormats =
        KnnVectorsFormat.availableKnnVectorsFormats().stream()
            .map(KnnVectorsFormat::forName)
            .filter(format -> supportsVectorEncoding(format, vectorEncoding))
            .filter(format -> supportsVectorSearch(format))
            .toList();
    return RandomPicks.randomFrom(random(), availableFormats);
  }

  /**
   * This is a test merge scheduler that will always use the intra merge executor to ensure we test
   * it.
   */
  static class TestConcurrentMergeScheduler extends ConcurrentMergeScheduler {
    @Override
    public Executor getIntraMergeExecutor(MergePolicy.OneMerge merge) {
      assert intraMergeExecutor != null : "scaledExecutor is not initialized";
      // Always do the intra merge executor to ensure we test it
      return intraMergeExecutor;
    }
  }
}
