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
import org.apache.lucene.util.BytesRef;

/** A per-document binary value. */
public abstract class BinaryDocValues extends DocValuesIterator {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected BinaryDocValues() {}

  /**
   * Returns the binary value for the current document ID. It is illegal to call this method after
   * {@link #advanceExact(int)} returned {@code false}.
   *
   * @return binary value
   */
  public abstract BytesRef binaryValue() throws IOException;

  /**
   * Bulk retrieval of binary doc values. This API helps reduce the performance impact of virtual
   * function calls.
   *
   * <p>This API behaves as if implemented as below, which is the default implementation:
   *
   * <pre><code class="language-java">
   * public void binaryValues(int size, int[] docs, BytesRef[] values) throws IOException {
   *   for (int i = 0; i &lt; size; ++i) {
   *     int doc = docs[i];
   *     if (advanceExact(doc)) {
   *       values[i] = BytesRef.deepCopyOf(binaryValue());
   *     } else {
   *       values[i] = null;
   *     }
   *   }
   * }
   * </code></pre>
   *
   * <p><b>NOTE</b>: The {@code docs} array is required to be sorted in ascending order with no
   * duplicates.
   *
   * <p><b>NOTE</b>: Documents that don't have a value for this field will have their corresponding
   * entry set to {@code null}. If you need to exclude documents that don't have a value, then you
   * could apply a {@link FieldExistsQuery} as a {@link Occur#FILTER} clause. Another option is to
   * fall back to using {@link #advanceExact} and {@link #binaryValue()} on ranges of doc IDs that
   * may not be dense, e.g.
   *
   * <pre><code class="language-java">
   * if (size &gt; 0 &amp;&amp; values.advanceExact(docs[0]) &amp;&amp; values.docIDRunEnd() &gt; docs[size - 1]) {
   *   // use values#binaryValues to retrieve values
   * } else {
   *   // some docs may not have a value, use #advanceExact and #binaryValue
   * }
   * </code></pre>
   *
   * <p><b>NOTE</b>: Each returned {@link BytesRef} is a deep copy owned by the caller and remains
   * valid after subsequent calls.
   *
   * @param size the number of values to retrieve
   * @param docs the buffer of doc IDs whose values should be looked up
   * @param values the buffer of values to fill; entries are set to {@code null} when a document
   *     doesn't have a value
   */
  public void binaryValues(int size, int[] docs, BytesRef[] values) throws IOException {
    binaryValues(size, docs, 0, values, 0);
  }

  /**
   * Offset-aware variant of {@link #binaryValues(int, int[], BytesRef[])}. Reads {@code size} doc
   * IDs starting at {@code docs[docsOffset]} and writes the corresponding values starting at {@code
   * values[valuesOffset]}. This follows the same convention as {@link System#arraycopy}.
   *
   * @param size the number of values to retrieve
   * @param docs the buffer of doc IDs whose values should be looked up
   * @param docsOffset first position in {@code docs} to read
   * @param values the buffer of values to fill; entries are set to {@code null} when a document
   *     doesn't have a value
   * @param valuesOffset first position in {@code values} to write
   */
  public void binaryValues(
      int size, int[] docs, int docsOffset, BytesRef[] values, int valuesOffset)
      throws IOException {
    for (int di = docsOffset, vi = valuesOffset, end = docsOffset + size; di < end; di++, vi++) {
      if (advanceExact(docs[di])) {
        values[vi] = BytesRef.deepCopyOf(binaryValue());
      } else {
        values[vi] = null;
      }
    }
  }
}
