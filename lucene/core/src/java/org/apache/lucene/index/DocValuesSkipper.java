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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;

/** Skipper for {@link DocValues}. */
public abstract class DocValuesSkipper {

  /**
   * Advance this skipper so that all levels contain {@code target}. The behavior is undefined if
   * {@code target} is greater than the number of documents in the index.
   */
  public abstract void advance(int target) throws IOException;

  /** Return the number of levels. This number may change when moving to a different interval. */
  public abstract int numLevels();

  /**
   * Return the minimum doc ID on the given level, inclusive. This returns {@code -1} if {@link
   * #advance(int)} has not been called yet and {@link DocIdSetIterator#NO_MORE_DOCS} if the
   * iterator is exhausted.
   */
  public abstract int minDocID(int level);

  /**
   * Return the maximum doc ID on the given level, inclusive. This returns {@code -1} if {@link
   * #advance(int)} has not been called yet and {@link DocIdSetIterator#NO_MORE_DOCS} if the
   * iterator is exhausted.
   */
  public abstract int maxDocID(int level);

  /** Return the minimum value on the given level, inclusive. */
  public abstract long minValue(int level);

  /** Return the maximum value on the given level, inclusive. */
  public abstract long maxValue(int level);

  /** Return the number of documents that have a value on the given level */
  public abstract int docCount(int level);

  /** Return the global minimum value. */
  public abstract long minValue();

  /** Return the global maximum value. */
  public abstract long maxValue();

  /** Return the global number of documents with a value for the field. */
  public abstract int docCount();
}
