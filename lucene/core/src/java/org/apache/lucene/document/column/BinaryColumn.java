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
package org.apache.lucene.document.column;

import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link Column} that provides variable-size binary values via a tuple cursor. Used for {@link
 * org.apache.lucene.index.DocValuesType#BINARY BINARY}, {@link
 * org.apache.lucene.index.DocValuesType#SORTED SORTED}, and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_SET SORTED_SET} doc values, and for stored/indexed
 * binary or text fields. Values fed to points are passed through unchanged, so callers are
 * responsible for producing sort-encoded bytes of the correct total length.
 *
 * <p>Numeric doc values ({@link org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} / {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}) and 1-D numeric points (int
 * / long / float / double) are fed by {@link LongColumn} instead.
 *
 * @lucene.experimental
 */
public abstract class BinaryColumn extends Column {

  /** Creates a BinaryColumn with the given field name, type, and density. */
  protected BinaryColumn(String name, IndexableFieldType fieldType, Density density) {
    super(name, fieldType, density);
  }

  /**
   * The {@link org.apache.lucene.document.StoredValue.Type} to emit when this column is written to
   * stored fields. The default is {@link org.apache.lucene.document.StoredValue.Type#BINARY}. Only
   * {@link org.apache.lucene.document.StoredValue.Type#BINARY} and {@link
   * org.apache.lucene.document.StoredValue.Type#STRING} are supported; numeric stored types require
   * {@link LongColumn}.
   */
  public StoredValue.Type storedType() {
    return StoredValue.Type.BINARY;
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract ObjectTupleCursor<BytesRef> tuples();

  /**
   * Returns a fresh values cursor iterating dense binary values for doc-ids {@code [0, numDocs)}.
   * Must be overridden when {@link Column#density()} is {@link Column.Density#DENSE DENSE}; the
   * default implementation throws {@link UnsupportedOperationException} and is never called for
   * {@link Column.Density#SPARSE SPARSE} columns.
   */
  public BytesRefValuesCursor values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }
}
