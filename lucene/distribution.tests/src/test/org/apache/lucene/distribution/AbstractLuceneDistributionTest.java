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
package org.apache.lucene.distribution;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;

/**
 * A parent scaffolding for tests that take a Lucene distribution as input. The location of the
 * distribution is pointed to by a system property {@link #DISTRIBUTION_PROPERTY}, which by default
 * is prepared and passed by the gradle build. It can be passed manually if you're testing from the
 * IDE, for example.
 *
 * <p>We do <em>not</em> want any distribution tests to depend on any Lucene classes (including the
 * test framework) so that there is no risk of accidental classpath space pollution. This also means
 * the default {@code LuceneTestCase} configuration setup is not used (you have to annotate test for
 * JUnit, for example).
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class AbstractLuceneDistributionTest extends RandomizedTest {
  /** A path to a directory with an expanded Lucene distribution. */
  public static final String DISTRIBUTION_PROPERTY = "lucene.distribution.dir";

  /** The expected distribution version of Lucene modules. */
  public static final String VERSION_PROPERTY = "lucene.distribution.version";

  /** Resolved and validated {@link #DISTRIBUTION_PROPERTY}. */
  private static Path distributionPath;

  // --------------------------------------------------------------------
  // Test groups, system properties and other annotations modifying tests
  // --------------------------------------------------------------------

  public static final String SYSPROP_REQUIRES_GUI = "tests.gui";

  /** Annotation for tests that requires GUI (physical or virtual display). */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_REQUIRES_GUI)
  public @interface RequiresGUI {}

  /** Ensure Lucene classes are not directly visible. */
  @BeforeClass
  public static void checkLuceneNotInClasspath() {
    Assertions.assertThatThrownBy(
            () -> {
              Class.forName("org.apache.lucene.index.IndexWriter");
            })
        .isInstanceOf(ClassNotFoundException.class);
  }

  /** Verify the distribution property is provided and points at a valid location. */
  @BeforeClass
  public static void parseExternalProperties() {
    String distributionPropertyValue = System.getProperty(DISTRIBUTION_PROPERTY);
    if (distributionPropertyValue == null) {
      throw new AssertionError(DISTRIBUTION_PROPERTY + " property is required for this test.");
    }

    distributionPath = Paths.get(distributionPropertyValue);

    // Ensure the distribution path is sort of valid.
    Path topLevelReadme = distributionPath.resolve("README.md");
    if (!Files.isRegularFile(topLevelReadme)) {
      throw new AssertionError(
          DISTRIBUTION_PROPERTY
              + " property does not seem to point to a top-level distribution directory"
              + " where this file is present: "
              + topLevelReadme.toAbsolutePath());
    }
  }

  protected static Path getDistributionPath() {
    return Objects.requireNonNull(distributionPath, "Distribution path not set?");
  }
}
