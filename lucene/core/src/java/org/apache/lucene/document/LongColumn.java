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
package org.apache.lucene.document;

import org.apache.lucene.index.IndexableFieldType;

/**
 * A {@link Column} that provides long values. Used for {@link
 * org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values and for stored
 * long fields.
 *
 * <p>Iteration is performed via cursors. {@link #tuples()} is always available and yields {@code
 * (docID, longValue)} pairs. {@link #values()} is a bulk cursor over consecutive doc-ids; it must
 * be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE} and is only consulted
 * in that case.
 *
 * @lucene.experimental
 */
public abstract class LongColumn extends Column {

  /** Creates a LongColumn with the given field name, type, and density. */
  protected LongColumn(String name, IndexableFieldType fieldType, Density density) {
    super(name, fieldType, density);
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract LongTupleCursor tuples();

  /**
   * Returns a fresh values cursor iterating dense long values for doc-ids {@code [0, numDocs)}.
   * Must be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE}; the default
   * implementation throws {@link UnsupportedOperationException} and is never called for {@link
   * Column.Density#SPARSE SPARSE} columns.
   */
  public LongValuesCursor values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }
}
