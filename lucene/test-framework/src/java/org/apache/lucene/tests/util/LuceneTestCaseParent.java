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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.util.InfoStream;
import org.junit.Assert;

/**
 * Private parent class for junit4 ({@link LuceneTestCase} and junit5 ({@link
 * LuceneTestCaseJupiter}).
 */
public abstract sealed class LuceneTestCaseParent extends Assert
    permits LuceneTestCase, LuceneTestCaseJupiter {

  // --------------------------------------------------------------------
  // Test groups, system properties and other annotations modifying tests
  // --------------------------------------------------------------------

  public static final String SYSPROP_NIGHTLY = "tests.nightly";
  public static final String SYSPROP_WEEKLY = "tests.weekly";
  public static final String SYSPROP_MONSTER = "tests.monster";
  public static final String SYSPROP_AWAITSFIX = "tests.awaitsfix";
  public static final String SYSPROP_MAXFAILURES = "tests.maxfailures";
  public static final String SYSPROP_FAILFAST = "tests.failfast";

  /**
   * True if and only if tests are run in verbose mode. If this flag is false tests are not expected
   * to print any messages. Enforced with {@link TestRuleLimitSysouts}.
   */
  public static final boolean VERBOSE = systemPropertyAsBoolean("tests.verbose", false);

  /** Enables or disables dumping of {@link InfoStream} messages. */
  public static final boolean INFOSTREAM = systemPropertyAsBoolean("tests.infostream", VERBOSE);

  /**
   * True if {@code tests.asserts} is enabled (either explicitly via the build option or, if not
   * present, by the default assertion status on this class).
   */
  public static final boolean TEST_ASSERTS_ENABLED =
      systemPropertyAsBoolean("tests.asserts", LuceneTestCase.class.desiredAssertionStatus());

  /**
   * The default (embedded resource) lines file.
   *
   * @see #TEST_LINE_DOCS_FILE
   */
  public static final String DEFAULT_LINE_DOCS_FILE = "europarl.lines.txt.gz";

  /**
   * Random sample from enwiki used in tests. See {@code help/tests.txt}. gradle task downloading
   * this data set: {@code gradlew getEnWikiRandomLines}.
   */
  public static final String JENKINS_LARGE_LINE_DOCS_FILE = "enwiki.random.lines.txt";

  /** Gets the codec to run tests with. */
  public static final String TEST_CODEC = System.getProperty("tests.codec", "random");

  /** Gets the postingsFormat to run tests with. */
  public static final String TEST_POSTINGSFORMAT =
      System.getProperty("tests.postingsformat", "random");

  /** Gets the docValuesFormat to run tests with */
  public static final String TEST_DOCVALUESFORMAT =
      System.getProperty("tests.docvaluesformat", "random");

  /** Gets the directory to run tests with */
  public static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");

  /** The line file used in tests (by {@link LineFileDocs}). */
  public static final String TEST_LINE_DOCS_FILE =
      System.getProperty("tests.linedocsfile", DEFAULT_LINE_DOCS_FILE);

  /** Whether or not {@link LuceneTestCase.Nightly} tests should run. */
  public static final boolean TEST_NIGHTLY =
      systemPropertyAsBoolean(
          SYSPROP_NIGHTLY, LuceneTestCase.Nightly.class.getAnnotation(TestGroup.class).enabled());

  /** Whether or not {@link LuceneTestCase.Weekly} tests should run. */
  public static final boolean TEST_WEEKLY =
      systemPropertyAsBoolean(
          SYSPROP_WEEKLY, LuceneTestCase.Weekly.class.getAnnotation(TestGroup.class).enabled());

  /** Whether or not {@link LuceneTestCase.Monster} tests should run. */
  public static final boolean TEST_MONSTER =
      systemPropertyAsBoolean(
          SYSPROP_MONSTER, LuceneTestCase.Monster.class.getAnnotation(TestGroup.class).enabled());

  /** Whether or not {@link LuceneTestCase.AwaitsFix} tests should run. */
  public static final boolean TEST_AWAITSFIX =
      systemPropertyAsBoolean(
          SYSPROP_AWAITSFIX,
          LuceneTestCase.AwaitsFix.class.getAnnotation(TestGroup.class).enabled());

  /**
   * Throttling, see {@link MockDirectoryWrapper#setThrottling(MockDirectoryWrapper.Throttling)}.
   */
  public static final MockDirectoryWrapper.Throttling TEST_THROTTLING =
      TEST_NIGHTLY
          ? MockDirectoryWrapper.Throttling.SOMETIMES
          : MockDirectoryWrapper.Throttling.NEVER;

  /**
   * A random multiplier which you should use when writing random tests: multiply it by the number
   * of iterations to scale your tests (for nightly builds).
   */
  public static final int RANDOM_MULTIPLIER =
      systemPropertyAsInt("tests.multiplier", defaultRandomMultiplier());

  /** Compute the default value of the random multiplier (based on {@link #TEST_NIGHTLY}). */
  static int defaultRandomMultiplier() {
    return TEST_NIGHTLY ? 2 : 1;
  }

  /** Leave temporary files on disk, even on successful runs. */
  public static final boolean LEAVE_TEMPORARY;

  static {
    boolean defaultValue = false;
    for (String property :
        Arrays.asList(
            "tests.leaveTemporary" /* ANT tasks' (junit4) flag. */,
            "tests.leavetemporary" /* lowercase */,
            "tests.leavetmpdir" /* default */)) {
      defaultValue |= systemPropertyAsBoolean(property, false);
    }
    LEAVE_TEMPORARY = defaultValue;
  }

  /** Filesystem-based {@link Directory} implementations. */
  protected static final List<String> FS_DIRECTORIES =
      Arrays.asList("NIOFSDirectory", "MMapDirectory");

  /** All {@link Directory} implementations. */
  protected static final List<String> CORE_DIRECTORIES;

  static {
    CORE_DIRECTORIES = new ArrayList<>(FS_DIRECTORIES);
    CORE_DIRECTORIES.add(ByteBuffersDirectory.class.getSimpleName());
  }

  /**
   * If specified, limits the number of method calls to each individual instance returned by {@link
   * #random()}.
   */
  public static final String SYSPROP_RANDOM_MAXCALLS = "tests.random.maxcalls";

  /** If specified, limits the number of calls {@link #random()} itself. */
  public static final String SYSPROP_RANDOM_MAXACQUIRES = "tests.random.maxacquires";

  /**
   * Returns a Random instance based on the current state of another Random. The returned instance
   * should be faster for thousands of consecutive calls because it doesn't assert that it isn't
   * shared between threads or used within the correct {@link RandomizedContext}.
   *
   * <p>Use this method for local tight loops that generate a lot of random data.
   */
  public static Random nonAssertingRandom(Random rnd) {
    return new Xoroshiro128PlusRandom(rnd.nextLong());
  }

  // -----------------------------------------------------------------
  // This bit is for supporting all the static method calls we have all over the place.
  // In order for both junit4 and junit5 to work, we need to make sure that each class
  // runs in isolation (so no concurrent tests at the moment) and that it provides
  // test-framework-dependent "infrastructure" like Random suppliers, test class names, etc.
  // to this class.
  // -----------------------------------------------------------------

  protected interface TestFrameworkInfra {
    Random threadRandom();

    <T extends Closeable> T closeAfterTest(T resource);

    <T extends Closeable> T closeAfterClass(T resource);

    void afterEach() throws IOException;

    void afterAll() throws IOException;

    SetupAndRestoreStaticEnv getClassEnv();
  }

  private static final AtomicReference<TestFrameworkInfra> testFrameworkInfra =
      new AtomicReference<>();

  protected static void setTestFrameworkInfra(
      TestFrameworkInfra expected, TestFrameworkInfra newInfra) {
    TestFrameworkInfra prev;
    if ((prev = testFrameworkInfra.compareAndExchange(expected, newInfra)) != expected) {
      throw new RuntimeException(
          "Test framework infrastructure is not set to the expected value: "
              + prev
              + ", expected: "
              + expected);
    }
  }

  protected static TestFrameworkInfra getTestFrameworkInfra() {
    return Objects.requireNonNull(
        testFrameworkInfra.get(), "Expected test framework not to be null.");
  }

  // -----------------------------------------------------------------
  // Truly immutable fields and constants, initialized once and valid
  // for all suites ever since.
  // -----------------------------------------------------------------

  /** A {@link org.apache.lucene.search.QueryCachingPolicy} that randomly caches. */
  public static final QueryCachingPolicy MAYBE_CACHE_POLICY =
      new QueryCachingPolicy() {
        @Override
        public void onUse(Query query) {}

        @Override
        public boolean shouldCache(Query query) {
          return getTestFrameworkInfra().threadRandom().nextBoolean();
        }
      };

  public static Random random() {
    return Objects.requireNonNull(testFrameworkInfra.get(), "Test framework not initialized?")
        .threadRandom();
  }

  /**
   * Registers a {@link Closeable} resource that should be closed after the test completes.
   *
   * @return <code>resource</code> (for call chaining).
   */
  public <T extends Closeable> T closeAfterTest(T resource) {
    return getTestFrameworkInfra().closeAfterTest(resource);
  }

  /**
   * Registers a {@link Closeable} resource that should be closed after the suite completes.
   *
   * @return <code>resource</code> (for call chaining).
   */
  public static <T extends Closeable> T closeAfterSuite(T resource) {
    return getTestFrameworkInfra().closeAfterClass(resource);
  }

  // TODO: to remove from here?

  /** Suite failure marker (any error in the test or suite scope). */
  @SuppressWarnings("NonFinalStaticField")
  protected static TestRuleMarkFailure suiteFailureMarker;

  static TestRuleTemporaryFilesCleanup tempFilesCleanupRule;
}
