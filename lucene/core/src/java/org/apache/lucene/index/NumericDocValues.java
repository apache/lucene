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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldExistsQuery;

/** A per-document numeric value. */
public abstract class NumericDocValues extends DocValuesIterator {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected NumericDocValues() {}

  /**
   * Returns the numeric value for the current document ID. It is illegal to call this method after
   * {@link #advanceExact(int)} returned {@code false}.
   *
   * @return numeric value
   */
  public abstract long longValue() throws IOException;

  /**
   * Bulk retrieval of numeric doc values. This API helps reduce the performance impact of virtual
   * function calls.
   *
   * <p>This API behaves as if implemented as below, which is the default implementation:
   *
   * <pre><code class="language-java">
   * public void longValues(int size, int[] docs, long[] values, long defaultValue) throws IOException {
   *   for (int i = 0; i &lt; size; ++i) {
   *     int doc = docs[i];
   *     long value;
   *     if (advanceExact(doc)) {
   *       value = longValue();
   *     } else {
   *       value = defaultValue;
   *     }
   *     values[i] = value;
   *   }
   * }
   * </code></pre>
   *
   * <p><b>NOTE</b>: The {@code docs} array is required to be sorted in ascending order with no
   * duplicates.
   *
   * <p><b>NOTE</b>: This API doesn't allow callers to know which doc IDs have a value or not. If
   * you need to exclude documents that don't have a value for this field, then you could apply a
   * {@link FieldExistsQuery} as a {@link Occur#FILTER} clause. Another option is to fall back to
   * using {@link #advanceExact} and {@link #longValue()} on ranges of doc IDs that may not be
   * dense, e.g.
   *
   * <pre><code class="language-java">
   * if (size > 0 &amp;&amp; values.advannceExact(docs[0]) &amp;&amp; values.docIDRunEnd() &gt; docs[size - 1]) {
   *   // use values#longValues to retrieve values
   * } else {
   *   // some docs may not have a value, use #advanceExact and #longValue
   * }
   * </code></pre>
   *
   * @param size the number of values to retrieve
   * @param docs the buffer of doc IDs whose values should be looked up
   * @param values the buffer of values to fill
   * @param defaultValue the value to put in the buffer when a document doesn't have a value
   */
  public void longValues(int size, int[] docs, long[] values, long defaultValue)
      throws IOException {
    for (int i = 0; i < size; ++i) {
      int doc = docs[i];
      long value;
      if (advanceExact(doc)) {
        value = longValue();
      } else {
        value = defaultValue;
      }
      values[i] = value;
    }
  }
}
