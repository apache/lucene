package org.apache.lucene.tests.util;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.Xoroshiro128PlusRandom;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Private parent class for junit4 ({@link LuceneTestCase} and junit5 ({@link
 * LuceneTestCaseJupiter}).
 */
abstract sealed class LuceneTestCaseParent extends Assert
    permits LuceneTestCase, LuceneTestCaseJupiter {
  private static volatile AtomicReference<Supplier<Random>> randomSupplier =
      new AtomicReference<>();

  static Supplier<Random> replaceRandomSupplier(Supplier<Random> rndSupplier) {
    return randomSupplier.getAndSet(rndSupplier);
  }

  /**
   * Access to the current {@link RandomizedContext}'s Random instance. It is safe to use this
   * method from multiple threads, etc., but it should be called while within a runner's scope (so
   * no static initializers). The returned {@link Random} instance will be <b>different</b> when
   * this method is called inside a {@link BeforeClass} hook (static suite scope) and within {@link
   * Before}/ {@link After} hooks or test methods.
   *
   * <p>The returned instance must not be shared with other threads or cross a single scope's
   * boundary. For example, a {@link Random} acquired within a test method shouldn't be reused for
   * another test case.
   *
   * <p>There is an overhead connected with getting the {@link Random} for a particular context and
   * thread. It is better to use a non-asserting {@link Random} instance locally if tight loops with
   * multiple invocations are present. See {@link #nonAssertingRandom(Random)}.
   */
  public static Random random() {
    return Objects.requireNonNull(randomSupplier.get()).get();
  }

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
}
