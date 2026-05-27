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

import java.util.regex.Pattern;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A JUnit5 {@link ExecutionCondition} that filters test methods based on the {@code tests.method}
 * system property. This mirrors the behavior of RandomizedRunner's {@code MethodGlobFilter} for
 * JUnit4 tests, ensuring that {@code --tests} filtering works consistently when {@code tests.iters}
 * is used.
 *
 * <p>When {@code tests.method} is set, only test methods whose names match the glob pattern (with
 * {@code *} as wildcard) will be executed. All other tests in the class are disabled.
 *
 * @lucene.internal
 */
class MethodFilterExtension implements ExecutionCondition {

  private static final String TESTS_METHOD_PROPERTY = "tests.method";

  /** Cached compiled pattern for the current tests.method value. */
  private static volatile Pattern cachedPattern;

  /** The tests.method value that cachedPattern was compiled from. */
  private static volatile String cachedPatternSource;

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    String methodFilter = System.getProperty(TESTS_METHOD_PROPERTY);
    if (methodFilter == null || methodFilter.isBlank()) {
      return ConditionEvaluationResult.enabled("No tests.method filter set");
    }

    // Only apply to test methods, not containers (classes)
    if (!context.getTestMethod().isPresent()) {
      return ConditionEvaluationResult.enabled("Not a test method");
    }

    String testMethodName = context.getTestMethod().get().getName();
    if (globMatches(methodFilter, testMethodName)) {
      return ConditionEvaluationResult.enabled(
          "Test method '"
              + testMethodName
              + "' matches tests.method filter '"
              + methodFilter
              + "'");
    }

    return ConditionEvaluationResult.disabled(
        "Test method '"
            + testMethodName
            + "' does not match tests.method filter '"
            + methodFilter
            + "'");
  }

  /**
   * Checks if a string matches any of the comma-separated simplified glob patterns where {@code *}
   * matches any sequence of characters and {@code ?} matches any single character.
   */
  private static boolean globMatches(String glob, String string) {
    Pattern pattern = compileGlobPattern(glob);
    return pattern.matcher(string).matches();
  }

  /** Compiles a glob pattern (with caching) to a regex Pattern. */
  private static Pattern compileGlobPattern(String glob) {
    // Double-checked locking for lazy initialization of the cached pattern.
    Pattern result = cachedPattern;
    if (result == null || !glob.equals(cachedPatternSource)) {
      synchronized (MethodFilterExtension.class) {
        result = cachedPattern;
        if (result == null || !glob.equals(cachedPatternSource)) {
          StringBuilder regex = new StringBuilder("^");
          for (String part : glob.split(",")) {
            if (regex.length() > 1) {
              regex.append("|");
            }
            regex.append("(?:");
            StringBuilder literalRun = new StringBuilder();
            for (int i = 0; i < part.length(); i++) {
              char c = part.charAt(i);
              switch (c) {
                case '*':
                  flushLiteralRun(regex, literalRun);
                  regex.append(".*");
                  break;
                case '?':
                  flushLiteralRun(regex, literalRun);
                  regex.append('.');
                  break;
                default:
                  literalRun.append(c);
                  break;
              }
            }
            flushLiteralRun(regex, literalRun);
            regex.append(')');
          }
          regex.append('$');
          result = Pattern.compile(regex.toString());
          cachedPattern = result;
          cachedPatternSource = glob;
        }
      }
    }
    return result;
  }

  /** Appends any accumulated literal characters to the regex, quoted as a group. */
  private static void flushLiteralRun(StringBuilder regex, StringBuilder literalRun) {
    if (literalRun.length() > 0) {
      regex.append(Pattern.quote(literalRun.toString()));
      literalRun.setLength(0);
    }
  }
}
