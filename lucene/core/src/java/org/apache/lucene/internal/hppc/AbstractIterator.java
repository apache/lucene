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

package org.apache.lucene.internal.hppc;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Simplifies the implementation of iterators a bit. Modeled loosely after Google Guava's API.
 *
 * <p>Forked from com.carrotsearch.hppc.AbstractIterator
 *
 * @lucene.internal
 */
public abstract class AbstractIterator<E> implements Iterator<E> {
  private static final int NOT_CACHED = 0;
  private static final int CACHED = 1;
  private static final int AT_END = 2;

  /** Current iterator state. */
  private int state = NOT_CACHED;

  /** The next element to be returned from {@link #next()} if fetched. */
  private E nextElement;

  @Override
  public boolean hasNext() {
    if (state == NOT_CACHED) {
      state = CACHED;
      nextElement = fetch();
    }
    return state == CACHED;
  }

  @Override
  public E next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    state = NOT_CACHED;
    return nextElement;
  }

  /** Default implementation throws {@link UnsupportedOperationException}. */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Fetch next element. The implementation must return {@link #done()} when all elements have been
   * fetched.
   *
   * @return Returns the next value for the iterator or chain-calls {@link #done()}.
   */
  protected abstract E fetch();

  /**
   * Call when done.
   *
   * @return Returns a unique sentinel value to indicate end-of-iteration.
   */
  protected final E done() {
    state = AT_END;
    return null;
  }
}
