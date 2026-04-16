package org.apache.lucene.tests.util;

import java.io.Closeable;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.lucene.util.IOUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Simulate global state for backward compatibility with static methods from {@link LuceneTestCase}
 * that are used all over the place.
 */
public final class GlobalStateSupport implements BeforeAllCallback, AfterAllCallback {
  private final ExtensionContext.Namespace NS =
      ExtensionContext.Namespace.create(getClass().getName());

  private static final class State {
    private ConcurrentHashMap<Thread, Random> perThreadRandoms;
    private Supplier<Random> previousSupplier;

    void reset() {
      perThreadRandoms = null;
      previousSupplier = null;
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

    Supplier<Random> rnd = getRandomSupplier(context.getExecutableInvoker());

    var perThreadRandoms = state.perThreadRandoms;
    state.previousSupplier =
        LuceneTestCaseParent.replaceRandomSupplier(
            () -> perThreadRandoms.computeIfAbsent(Thread.currentThread(), _ -> rnd.get()));
  }

  // This trick is needed to get the Supplier<Random> injected by a parameter resolved
  // of the randomized testing framework.
  private Supplier<Random> getRandomSupplier(ExecutableInvoker executableInvoker) throws Exception {
    var hack =
        new Object() {
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

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    var state = getState(context);
    try {
      LuceneTestCaseParent.replaceRandomSupplier(state.previousSupplier);
      IOUtils.close(
          state.perThreadRandoms.values().stream()
              .filter(rnd1 -> rnd1 instanceof Closeable)
              .map(rnd1 -> (Closeable) rnd1)
              .toList());
    } finally {
      state.reset();
    }
  }
}
