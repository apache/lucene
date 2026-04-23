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

/**
 * A {@link Column} that provides variable-size binary values via a tuple cursor. Used for {@link
 * org.apache.lucene.index.DocValuesType#BINARY BINARY}, {@link
 * org.apache.lucene.index.DocValuesType#SORTED SORTED}, and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_SET SORTED_SET} doc values, and for stored/indexed
 * binary or text fields.
 *
 * <p>Numeric doc values ({@link org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} / {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}) and points require {@link
 * NumericBinaryColumn} instead, which adds fixed-size, byte order, and a numeric kind.
 *
 * @lucene.experimental
 */
public abstract class BinaryColumn extends Column {

  /** Creates a BinaryColumn with the given field name, type, and density. */
  protected BinaryColumn(String name, IndexableFieldType fieldType, Density density) {
    super(name, fieldType, density);
  }

  /**
   * The {@link StoredValue.Type} to emit when this column is written to stored fields. The default
   * is {@link StoredValue.Type#BINARY}. On a plain {@link BinaryColumn}, only {@link
   * StoredValue.Type#BINARY} and {@link StoredValue.Type#STRING} are supported; numeric stored
   * types require {@link NumericBinaryColumn}.
   */
  public StoredValue.Type storedType() {
    return StoredValue.Type.BINARY;
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract BinaryTupleCursor tuples();
}
