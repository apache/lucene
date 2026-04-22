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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.lucene.util.IOUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Simulate global state for backward compatibility with static methods from {@link LuceneTestCase}
 * that are used all over the place.
 */
public final class GlobalStateSupport
    implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {
  private final ExtensionContext.Namespace NS =
      ExtensionContext.Namespace.create(getClass().getName());

  private static final class State {
    private LuceneTestCaseParent.TestFrameworkInfra frameworkInfra;
    private TestRuleSetupAndRestoreClassEnv prevClassEnvRule;

    void reset() {
      frameworkInfra = null;
    }

    void initialize() {
      if (frameworkInfra != null) {
        throw new RuntimeException();
      }
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    State state = getState(context);
    state.initialize();

    // touch LuceneTestCase to trigger static initializers.
    LuceneTestCase.ensureInitialized();

    state.frameworkInfra =
        new LuceneTestCaseParent.TestFrameworkInfra() {
          private final ConcurrentHashMap<Thread, Random> perThreadRandoms =
              new ConcurrentHashMap<>();

          private final List<Closeable> closeAfterTest = new ArrayList<>();
          private final List<Closeable> closeAfterSuite = new ArrayList<>();

          final Supplier<Random> rnd = getRandomSupplier(context.getExecutableInvoker());

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
          public void afterEach() throws IOException {
            synchronized (this) {
              IOUtils.close(closeAfterTest);
              closeAfterTest.clear();
            }
          }

          @Override
          public void afterAll() throws IOException {
            synchronized (this) {
              IOUtils.close(
                  Stream.of(closeAfterTest, closeAfterSuite, perThreadRandoms.values())
                      .flatMap(Collection::stream)
                      .filter(v -> v instanceof Closeable)
                      .map(v -> (Closeable) v)
                      .toList());
            }
          }
        };

    LuceneTestCaseParent.setTestFrameworkInfra(null, state.frameworkInfra);

    state.prevClassEnvRule = LuceneTestCase.classEnvRule;
    var targetClass = context.getRequiredTestClass();
    LuceneTestCase.classEnvRule =
        new TestRuleSetupAndRestoreClassEnv(state.frameworkInfra::threadRandom, () -> targetClass);
    LuceneTestCase.classEnvRule.before();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    getState(context).frameworkInfra.afterEach();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    var state = getState(context);
    var rule = LuceneTestCaseParent.classEnvRule;
    try {
      LuceneTestCaseParent.classEnvRule = state.prevClassEnvRule;
      state.frameworkInfra.afterAll();
    } finally {
      rule.after();
      LuceneTestCaseParent.setTestFrameworkInfra(state.frameworkInfra, null);
      state.reset();
    }
  }

  private State getState(ExtensionContext context) {
    return context
        .getStore(ExtensionContext.StoreScope.EXECUTION_REQUEST, NS)
        .computeIfAbsent(State.class);
  }

  // This trick is needed to get the Supplier<Random> injected by a parameter resolved
  // of the randomized testing framework.
  private Supplier<Random> getRandomSupplier(ExecutableInvoker executableInvoker) throws Exception {
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
