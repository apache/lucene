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
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;

import com.carrotsearch.randomizedtesting.jupiter.SysProps;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.testkit.engine.EngineTestKit;

/// groups all tests of the junit5/ jupiter parent class and its infrastructure.
@Execution(value = ExecutionMode.SAME_THREAD, reason = "single-threaded.")
public class TestLuceneTestCaseJupiter {
  /// Test cases should use parameter-injected [java.util.Random] or a supplier
  /// of [java.util.Random]. Avoid using static methods.
  @Nested
  class RandomInjection {
    @Test
    public void testRandomParameterInjected() {
      testKitBuilder(T1.class)
          .configurationParameter(SysProps.TESTS_SEED.propertyKey, "dead:beef:cafe")
          .execute()
          .allEvents()
          .assertThatEvents()
          .doNotHave(event(finishedWithFailure()));
    }

    static class T1 extends LuceneTestCaseJupiter {
      public T1(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @BeforeAll
      static void beforeAll(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @BeforeEach
      void beforeEach(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @Test
      void testMethod(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @Test
      void testMethodWithSupplier(Supplier<Random> supplier) {
        Assert.assertNotNull(supplier);
        Assert.assertNotNull(supplier.get());
      }

      @AfterEach
      void afterEach(Random rnd) {
        Assert.assertNotNull(rnd);
      }

      @AfterAll
      static void afterAll(Random rnd) {
        Assert.assertNotNull(rnd);
      }
    }
  }

  public static EngineTestKit.Builder testKitBuilder(Class<?> testClass) {
    return testKitBuilder().selectors(selectClass(testClass));
  }

  public static EngineTestKit.Builder testKitBuilder() {
    return EngineTestKit.engine("junit-jupiter");
  }
}
