package org.apache.lucene.tests.util;

import java.io.Closeable;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.lucene.util.IOUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class GlobalStaticRandomAccess
    implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {
  private ConcurrentHashMap<Thread, Random> perThreadRandoms;
  private Supplier<Random> previousSupplier;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (perThreadRandoms != null) {
      throw new RuntimeException(
          "Expected perThreadRandoms to be null (single-threaded, sequential test execution).");
    }
    perThreadRandoms = new ConcurrentHashMap<>();

    // This trick is needed to get the Supplier<Random> injected by a parameter resolved
    // of the randomized testing framework.
    @SuppressWarnings("unchecked")
    Supplier<Random> rnd =
        (Supplier<Random>)
            Objects.requireNonNull(
                context
                    .getExecutableInvoker()
                    .invoke(getClass().getMethod("beforeAll0", Supplier.class), this));

    previousSupplier =
        LuceneTestCase.replaceRandomSupplier(
            () -> perThreadRandoms.computeIfAbsent(Thread.currentThread(), _ -> rnd.get()));
  }

  public Supplier<Random> beforeAll0(Supplier<Random> rnd) {
    return rnd;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    // TODO: push test context?
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    // TODO: push test context?
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    try {
      LuceneTestCase.replaceRandomSupplier(previousSupplier);
      IOUtils.close(
          perThreadRandoms.values().stream()
              .filter(rnd1 -> rnd1 instanceof Closeable)
              .map(rnd1 -> (Closeable) rnd1)
              .toList());
    } finally {
      perThreadRandoms = null;
      previousSupplier = null;
    }
  }
}
