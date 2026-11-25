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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOIntConsumer;

/**
 * A stream of doc IDs. Doc IDs may be consumed at most once from a given stream. Use {@link
 * #fork()} to get an independent stream over the same doc IDs.
 *
 * @see LeafCollector#collect(DocIdStream)
 * @lucene.experimental
 */
public abstract class DocIdStream {

  /** Sole constructor, for invocation by sub classes. */
  protected DocIdStream() {}

  /**
   * Iterate over doc IDs contained in this stream in order, calling the given {@link IOIntConsumer}
   * on them. This is a terminal operation.
   */
  public void forEach(IOIntConsumer consumer) throws IOException {
    forEach(DocIdSetIterator.NO_MORE_DOCS, consumer);
  }

  /**
   * Iterate over doc IDs contained in this doc ID stream up to the given {@code upTo} exclusive,
   * calling the given {@link IOIntConsumer} on them. It is not possible to iterate these doc IDs
   * again later on.
   */
  public abstract void forEach(int upTo, IOIntConsumer consumer) throws IOException;

  /** Count the number of entries in this stream. This is a terminal operation. */
  public int count() throws IOException {
    return count(DocIdSetIterator.NO_MORE_DOCS);
  }

  /**
   * Count the number of doc IDs in this stream that are below the given {@code upTo}. These doc IDs
   * may not be consumed again later.
   */
  // Note: it's abstract rather than having a default impl that delegates to #forEach because doing
  // so would defeat the purpose of collecting hits via a DocIdStream.
  public abstract int count(int upTo) throws IOException;

  /**
   * Copy some matching doc IDs into the provided array and return the number of copied elements. A
   * return value of {@code 0} indicates that there are no remaining doc IDs. The given array must
   * not be empty.
   */
  public int intoArray(int[] array) {
    return intoArray(DocIdSetIterator.NO_MORE_DOCS, array);
  }

  /**
   * Copy some matching doc IDs under {@code upTo} (exclusive) into the provided array and return
   * the number of copied elements. A return value of {@code 0} indicates that there are no matching
   * doc IDs under {@code upTo} anymore. The given array must not be empty.
   */
  public abstract int intoArray(int upTo, int[] array);

  /**
   * Load doc IDs below {@code upTo} into the provided bit set.
   *
   * @param upTo exclusive upper bound of doc IDs to load
   * @param bitSet the bit set to fill
   * @param offset subtracted from doc id before setting bits
   */
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    forEach(upTo, doc -> bitSet.set(doc - offset));
  }

  /**
   * Return {@code true} if this stream may have remaining doc IDs. This must eventually return
   * {@code false} when the stream is exhausted.
   */
  public abstract boolean mayHaveRemaining();

  /**
   * Return a new stream that is positioned at the same point as this stream and returns the same
   * remaining doc IDs, or {@code null} if this stream doesn't support forking. The returned stream
   * and this stream can then be consumed independently: consuming one doesn't advance the other.
   *
   * <p>A forked stream is only valid as long as this stream is valid, typically until the enclosing
   * call to {@link LeafCollector#collect(DocIdStream)} returns.
   *
   * <p>This is useful for collectors that delegate to multiple collectors, such as {@link
   * MultiCollector}, so that every delegate can consume doc IDs in bulk. Implementations should
   * only override this method if they can fork cheaply, typically without copying doc IDs.
   *
   * <p>The default implementation returns {@code null}.
   */
  public DocIdStream fork() {
    return null;
  }
}
