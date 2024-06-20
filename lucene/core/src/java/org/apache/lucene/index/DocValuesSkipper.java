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

/**
 * Skipper for {@link DocValues}.
 *
 * <p>A skipper has a position that can only be advanced via {@link #advance(int)}. The next advance
 * position must be greater than {@link #maxDocID(int)} at level 0. A skipper's position, along with
 * a {@code level}, determines the interval at which the skipper is currently situated.
 */
public abstract class DocValuesSkipper {

  /**
   * Advance this skipper so that all levels contain the next document on or after {@code target}.
   *
   * <p><b>NOTE</b>: The behavior is undefined if {@code target} is less than or equal to {@code
   * maxDocID(0)}.
   *
   * <p><b>NOTE</b>: {@code minDocID(0)} may return a doc ID that is greater than {@code target} if
   * the target document doesn't have a value.
   */
  public abstract void advance(int target) throws IOException;

  /** Return the number of levels. This number may change when moving to a different interval. */
  public abstract int numLevels();

  /**
   * Return the minimum doc ID of the interval on the given level, inclusive. This returns {@code
   * -1} if {@link #advance(int)} has not been called yet and {@link DocIdSetIterator#NO_MORE_DOCS}
   * if the iterator is exhausted. This method is non-increasing when {@code level} increases. Said
   * otherwise {@code minDocID(level+1) <= minDocId(level)}.
   */
  public abstract int minDocID(int level);

  /**
   * Return the maximum doc ID of the interval on the given level, inclusive. This returns {@code
   * -1} if {@link #advance(int)} has not been called yet and {@link DocIdSetIterator#NO_MORE_DOCS}
   * if the iterator is exhausted. This method is non-decreasing when {@code level} decreases. Said
   * otherwise {@code maxDocID(level+1) >= maxDocId(level)}.
   */
  public abstract int maxDocID(int level);

  /**
   * Return the minimum value of the interval at the given level, inclusive.
   *
   * <p><b>NOTE</b>: It is only guaranteed that values in this interval are greater than or equal
   * the returned value. There is no guarantee that one document actually has this value.
   */
  public abstract long minValue(int level);

  /**
   * Return the maximum value of the interval at the given level, inclusive.
   *
   * <p><b>NOTE</b>: It is only guaranteed that values in this interval are less than or equal the
   * returned value. There is no guarantee that one document actually has this value.
   */
  public abstract long maxValue(int level);

  /**
   * Return the number of documents that have a value in the interval associated with the given
   * level.
   */
  public abstract int docCount(int level);

  /**
   * Return the global minimum value.
   *
   * <p><b>NOTE</b>: It is only guaranteed that values are greater than or equal the returned value.
   * There is no guarantee that one document actually has this value.
   */
  public abstract long minValue();

  /**
   * Return the global maximum value.
   *
   * <p><b>NOTE</b>: It is only guaranteed that values are less than or equal the returned value.
   * There is no guarantee that one document actually has this value.
   */
  public abstract long maxValue();

  /** Return the global number of documents with a value for the field. */
  public abstract int docCount();
}
