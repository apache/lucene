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

/**
 * A stream of doc IDs. Doc IDs may be consumed at most once.
 *
 * @see LeafCollector#collect(DocIdStream)
 * @lucene.experimental
 */
public abstract class DocIdStream {

  /** Sole constructor, for invocation by sub classes. */
  protected DocIdStream() {}

  /**
   * Iterate over doc IDs contained in this stream in order, calling the given {@link
   * CheckedIntConsumer} on them. This is a terminal operation.
   */
  public void forEach(CheckedIntConsumer<IOException> consumer) throws IOException {
    forEach(DocIdSetIterator.NO_MORE_DOCS, consumer);
  }

  /**
   * Iterate over doc IDs contained in this doc ID stream up to the given {@code upTo} exclusive,
   * calling the given {@link CheckedIntConsumer} on them. It is not possible to iterate these doc
   * IDs again later on.
   */
  public abstract void forEach(int upTo, CheckedIntConsumer<IOException> consumer)
      throws IOException;

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
   * Return {@code true} if this stream may have remaining doc IDs. This must eventually return
   * {@code false} when the stream is exhausted.
   */
  public abstract boolean mayHaveRemaining();
}
