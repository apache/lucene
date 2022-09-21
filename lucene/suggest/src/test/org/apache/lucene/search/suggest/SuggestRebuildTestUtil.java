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
package org.apache.lucene.search.suggest;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.BytesRef;

/** Reusable Logic for confirming that Lookup impls can return suggestions during a 'rebuild' */
public final class SuggestRebuildTestUtil {

  /**
   * Given a {@link Lookup} impl and some assertion callbacks, confirms that assertions which pass
   * after an initial build will continue to pass during a (slow) rebuild w/new data (in a
   * background thread), and that (optional) new assertions will pass once the rebuild is complete
   *
   * @param suggester to be tested
   * @param initialData initial data to use for initial {@link Lookup#build}
   * @param initialChecks assertions to test after the initial build, and during the re-{@link
   *     Lookup#build}
   * @param extraData will be aded to <code>initialData</code> and used to re-<code>build()</code>
   *     the suggester
   * @param finalChecks assertions to test after the re-<code>build()</code> completes
   */
  public static void testLookupsDuringReBuild(
      final Lookup suggester,
      final List<Input> initialData,
      final ExceptionalCallback initialChecks,
      final List<Input> extraData,
      final ExceptionalCallback finalChecks)
      throws Exception {
    // copy we can mutate
    final List<Input> data = new ArrayList<>(initialData);
    suggester.build(new InputArrayIterator(data));

    // sanity check initial results
    initialChecks.check(suggester);

    // modify source data we're going to build from, and spin up background thread that
    // will rebuild (slowly)
    data.addAll(extraData);
    final Semaphore readyToCheck = new Semaphore(0);
    final Semaphore readyToAdvance = new Semaphore(0);
    final AtomicReference<Throwable> buildError = new AtomicReference<>();
    final Thread rebuilder =
        new Thread(
            () -> {
              try {
                suggester.build(
                    new DelayedInputIterator(
                        readyToCheck, readyToAdvance, new InputArrayIterator(data.iterator())));
              } catch (Throwable t) {
                buildError.set(t);
                readyToCheck.release(data.size() * 100); // flood the semaphore so we don't block
              }
            });
    rebuilder.start();
    // at every stage of the slow rebuild, we should still be able to get our original suggestions
    // (+1 iteration to ensure final next() call can return null)
    for (int i = 0; i < data.size() + 1; i++) {
      try {
        assertNull(buildError.get());
        readyToCheck.acquire();
        initialChecks.check(suggester);
        readyToAdvance.release();
      } catch (Throwable t) {
        readyToAdvance.release(data.size() * 100); // flood the semaphore so we don't block
        throw t;
      }
    }
    // once all the data is released from the iterator, the background rebuild should finish, and
    // suggest results
    // should change
    rebuilder.join();
    assertNull(buildError.get());
    finalChecks.check(suggester);
  }

  /**
   * Simple marker interface to allow {@link #testLookupsDuringReBuild} callbacks to throw
   * Exceptions
   */
  public static interface ExceptionalCallback {
    public void check(final Lookup suggester) throws Exception;
  }

  /**
   * An InputArrayIterator wrapper whose {@link InputIterator#next} method releases on a Semaphore,
   * and then acquires from a differnet Semaphore.
   */
  private static final class DelayedInputIterator implements InputIterator {
    final Semaphore releaseOnNext;
    final Semaphore acquireOnNext;
    final InputIterator inner;

    public DelayedInputIterator(
        final Semaphore releaseOnNext, final Semaphore acquireOnNext, final InputIterator inner) {
      assert null != releaseOnNext;
      assert null != acquireOnNext;
      assert null != inner;
      this.releaseOnNext = releaseOnNext;
      this.acquireOnNext = acquireOnNext;
      this.inner = inner;
    }

    @Override
    public BytesRef next() throws IOException {
      releaseOnNext.release();
      try {
        acquireOnNext.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return inner.next();
    }

    @Override
    public long weight() {
      return inner.weight();
    }

    @Override
    public BytesRef payload() {
      return inner.payload();
    }

    @Override
    public boolean hasPayloads() {
      return inner.hasPayloads();
    }

    @Override
    public Set<BytesRef> contexts() {
      return inner.contexts();
    }

    @Override
    public boolean hasContexts() {
      return inner.hasContexts();
    }
  }
}
