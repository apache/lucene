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
import java.util.ArrayList;
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
    private TestRuleSetupAndRestoreClassEnv prevClassEnvRule;

    private ConcurrentHashMap<Thread, Random> perThreadRandoms;
    private Supplier<Random> previousSupplier;

    private final List<Closeable> closeAfterTest = new ArrayList<>();
    private final List<Closeable> closeAfterSuite = new ArrayList<>();

    void reset() {
      perThreadRandoms = null;
      previousSupplier = null;
      closeAfterTest.clear();
      closeAfterSuite.clear();
    }

    void initialize() {
      if (perThreadRandoms != null) {
        throw new RuntimeException(
            "Expected perThreadRandoms to be null (single-threaded, sequential test execution).");
      }
      perThreadRandoms = new ConcurrentHashMap<>();
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    State state = getState(context);
    state.initialize();

    if (LuceneTestCase.VERBOSE) {
      // touch LuceneTestCase to trigger static initializers.
      // TODO: perhaps we should run the entire rule chain instead...
    }

    Supplier<Random> rnd = getRandomSupplier(context.getExecutableInvoker());

    var perThreadRandoms = state.perThreadRandoms;
    state.previousSupplier =
        LuceneTestCaseParent.replaceRandomSupplier(
            () -> perThreadRandoms.computeIfAbsent(Thread.currentThread(), _ -> rnd.get()));

    LuceneTestCaseParent.closeAfter.set(
        new LuceneTestCaseParent.CloseAfterHook() {
          @Override
          public <T extends Closeable> T closeAfterTest(T resource) {
            synchronized (state) {
              state.closeAfterTest.add(resource);
            }
            return resource;
          }

          @Override
          public <T extends Closeable> T closeAfterSuite(T resource) {
            synchronized (state) {
              state.closeAfterSuite.add(resource);
            }
            return resource;
          }
        });

    state.prevClassEnvRule = LuceneTestCase.classEnvRule;
    var targetClass = context.getRequiredTestClass();
    LuceneTestCase.classEnvRule = new TestRuleSetupAndRestoreClassEnv(rnd, () -> targetClass);
    LuceneTestCase.classEnvRule.before();
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    var state = getState(context);
    List<Closeable> toClose;
    synchronized (state) {
      toClose = List.copyOf(state.closeAfterTest);
    }
    IOUtils.close(toClose);
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    var state = getState(context);
    var rule = LuceneTestCaseParent.classEnvRule;
    try {
      LuceneTestCaseParent.replaceRandomSupplier(state.previousSupplier);
      LuceneTestCaseParent.classEnvRule = state.prevClassEnvRule;
      IOUtils.close(
          Stream.concat(
                  state.closeAfterSuite.stream(),
                  state.perThreadRandoms.values().stream()
                      .filter(rnd1 -> rnd1 instanceof Closeable)
                      .map(rnd1 -> (Closeable) rnd1))
              .toList());
    } finally {
      rule.after();
      state.reset();
    }
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

  private State getState(ExtensionContext context) {
    return context
        .getStore(ExtensionContext.StoreScope.EXECUTION_REQUEST, NS)
        .computeIfAbsent(State.class);
  }
}
