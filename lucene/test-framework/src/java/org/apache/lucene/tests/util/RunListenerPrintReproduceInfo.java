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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import java.util.Optional;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * A suite listener printing a "reproduce string" (junit4/ randomizedtesting). This ensures test
 * result events are always captured properly even if exceptions happen at initialization or suite/
 * hooks level.
 */
public final class RunListenerPrintReproduceInfo extends RunListener {
  /** The currently executing scope. */
  private LifecycleScope scope;

  /** Current test failed. */
  private boolean testFailed;

  /** Suite-level code (initialization, rule, hook) failed. */
  private boolean suiteFailed;

  /** Either a test or something else has failed. */
  private boolean somethingFailed;

  /** true if we should skip the reproduce string (diagnostics are independent) */
  private boolean suppressReproduceLine;

  /** Environment settings for the run. Set by {@link LuceneTestCase}. */
  @SuppressWarnings("NonFinalStaticField")
  static TestEnvInfo envInfoJunit4;

  @Override
  public void testRunStarted(Description description) throws Exception {
    suiteFailed = false;
    testFailed = false;
    scope = LifecycleScope.SUITE;
    envInfoJunit4 = null;

    suppressReproduceLine =
        RandomizedContext.current()
            .getTargetClass()
            .isAnnotationPresent(LuceneTestCase.SuppressReproduceLine.class);
  }

  @Override
  public void testStarted(Description description) throws Exception {
    this.testFailed = false;
    this.scope = LifecycleScope.TEST;
  }

  @Override
  public void testFailure(Failure failure) throws Exception {
    if (scope == LifecycleScope.TEST) {
      testFailed = true;
    } else {
      suiteFailed = true;
    }
    somethingFailed = true;
  }

  @Override
  public void testFinished(Description description) throws Exception {
    if (testFailed && !suppressReproduceLine) {
      System.err.println(
          envInfoJunit4.getAdditionalFailureInfo(
              RandomizedContext.current().getRunnerSeedAsString(),
              b -> {
                appendSelectorArguments(
                    b, Optional.of(stripTestNameAugmentations(description.getMethodName())));
              }));
    }
    scope = LifecycleScope.SUITE;
    testFailed = false;
  }

  private void appendSelectorArguments(StringBuilder b, Optional<String> testMethod) {
    // Figure out the test case name and method, if any.
    String testClass = RandomizedContext.current().getTargetClass().getSimpleName();
    b.append("--tests ");
    b.append(testClass);
    if (testMethod.isPresent()) {
      b.append(".").append(testMethod.get());
    }
  }

  /**
   * The {@link Description} object in JUnit does not expose the actual test method, instead it has
   * the concept of a unique "name" of a test. To run the same method (tests) repeatedly,
   * randomizedtesting must make those "names" unique: it appends the current iteration and seeds to
   * the test method's name. We strip this information here.
   */
  private String stripTestNameAugmentations(String methodName) {
    if (methodName != null) {
      methodName = methodName.replaceAll("\\s*\\{.+?\\}", "");
    }
    return methodName;
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    if (suiteFailed && !suppressReproduceLine) {
      System.err.println(
          envInfoJunit4.getAdditionalFailureInfo(
              RandomizedContext.current().getRunnerSeedAsString(),
              b -> {
                appendSelectorArguments(b, Optional.empty());
              }));
    }
    if (somethingFailed || LuceneTestCase.VERBOSE) {
      System.err.println(envInfoJunit4.getDebuggingInformation());
    }
  }
}
