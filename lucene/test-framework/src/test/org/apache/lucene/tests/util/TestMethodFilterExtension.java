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

import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

/// Tests for {@link MethodFilterExtension}.
///
/// <p>Note: {@link LuceneTestCaseJupiter} adds 3 infrastructure tests
/// (verifyTestAssertionStatus, enforceClassNamingConvention, allTestMethodsAreAnnotated)
/// to every subclass, so the inner test class below has 6 tests total.
public class TestMethodFilterExtension {

  /// Total number of tests in {@link FilterableTestClass} (3 parent + 3 own).
  private static final int TOTAL_TESTS = 6;

  /// A test class with methods that can be filtered.
  static class FilterableTestClass extends LuceneTestCaseJupiter {
    @org.junit.jupiter.api.Test
    void testMethodA() {}

    @org.junit.jupiter.api.Test
    void testMethodB() {}

    @org.junit.jupiter.api.Test
    void otherMethod() {}
  }

  @Test
  public void testNoFilterRunsAllTests() {
    System.clearProperty("tests.method");
    Events tests = runTests(FilterableTestClass.class);
    tests.assertStatistics(stats -> stats.started(TOTAL_TESTS).succeeded(TOTAL_TESTS));
  }

  @Test
  public void testSinglePatternFilter() {
    System.setProperty("tests.method", "testMethod*");
    try {
      Events tests = runTests(FilterableTestClass.class);
      // testMethodA and testMethodB match; 4 others are skipped.
      tests.assertStatistics(stats -> stats.started(2).succeeded(2).skipped(4));
    } finally {
      System.clearProperty("tests.method");
    }
  }

  @Test
  public void testExactMethodFilter() {
    System.setProperty("tests.method", "testMethodA");
    try {
      Events tests = runTests(FilterableTestClass.class);
      // Only testMethodA matches; 5 others are skipped.
      tests.assertStatistics(stats -> stats.started(1).succeeded(1).skipped(5));
    } finally {
      System.clearProperty("tests.method");
    }
  }

  @Test
  public void testCommaSeparatedPatterns() {
    System.setProperty("tests.method", "testMethodA,testMethodB");
    try {
      Events tests = runTests(FilterableTestClass.class);
      // testMethodA and testMethodB match; 4 others are skipped.
      tests.assertStatistics(stats -> stats.started(2).succeeded(2).skipped(4));
    } finally {
      System.clearProperty("tests.method");
    }
  }

  @Test
  public void testQuestionMarkWildcard() {
    System.setProperty("tests.method", "testMethod?");
    try {
      Events tests = runTests(FilterableTestClass.class);
      // testMethodA and testMethodB match (the ? matches the last char); 4 others are skipped.
      tests.assertStatistics(stats -> stats.started(2).succeeded(2).skipped(4));
    } finally {
      System.clearProperty("tests.method");
    }
  }

  @Test
  public void testNonMatchingFilter() {
    System.setProperty("tests.method", "nonExistent*");
    try {
      Events tests = runTests(FilterableTestClass.class);
      // Nothing matches; all 6 are skipped.
      tests.assertStatistics(stats -> stats.started(0).skipped(TOTAL_TESTS));
    } finally {
      System.clearProperty("tests.method");
    }
  }

  private static Events runTests(Class<?> testClass) {
    return EngineTestKit.engine("junit-jupiter")
        .selectors(selectClass(testClass))
        .execute()
        .testEvents();
  }
}
