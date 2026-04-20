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

import java.nio.ByteOrder;
import org.apache.lucene.index.IndexableFieldType;

/**
 * A {@link BinaryColumn} whose values are fixed-size numeric encodings (int / long / IEEE float /
 * IEEE double). This is the column type used for {@link
 * org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} / {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values and for points.
 *
 * <p>Bytes are interpreted with {@link #byteOrder()}. The numeric-doc-values path writes the
 * decoded long as-is. The points path additionally sortable-encodes each value per {@link
 * #numericKind()} before calling {@link org.apache.lucene.index.PointValues}.
 *
 * <p>Stored fields are supported. {@link #storedType()} defaults to the numeric variant that
 * matches {@link #numericKind()} ({@code INT→INTEGER}, {@code LONG→LONG}, {@code FLOAT→FLOAT},
 * {@code DOUBLE→DOUBLE}); override to emit {@link StoredValue.Type#BINARY BINARY} (raw bytes) or
 * {@link StoredValue.Type#STRING STRING} (UTF-8) instead. Numeric variants decode the raw bytes
 * with {@link #byteOrder()} at stored-field emission time (using {@link Float#intBitsToFloat(int)}
 * / {@link Double#longBitsToDouble(long)} for the IEEE variants).
 *
 * <p>Non-numeric binary data (BINARY/SORTED/SORTED_SET doc values, variable-size stored/indexed
 * fields) should use plain {@link BinaryColumn} directly.
 *
 * @lucene.experimental
 */
public abstract class NumericBinaryColumn extends BinaryColumn {

  /**
   * The numeric interpretation of the column's bytes. The bytes are decoded with {@link
   * #byteOrder()} into a long (or int widened to long); for points the chain then computes the
   * sortable-byte form required by {@link org.apache.lucene.index.PointValues}.
   */
  public enum NumericKind {
    /** Signed 32-bit int. Requires {@link #fixedSize()} == 4. */
    INT,
    /** Signed 64-bit long. Requires {@link #fixedSize()} == 8. */
    LONG,
    /** IEEE 754 32-bit float. Requires {@link #fixedSize()} == 4. */
    FLOAT,
    /** IEEE 754 64-bit double. Requires {@link #fixedSize()} == 8. */
    DOUBLE,
  }

  /** Creates a NumericBinaryColumn with the given field name and type. */
  protected NumericBinaryColumn(String name, IndexableFieldType fieldType) {
    super(name, fieldType);
  }

  /**
   * Returns the fixed byte length of every value. Must be {@code 4} (for INT / FLOAT) or {@code 8}
   * (for LONG / DOUBLE), matching {@link #numericKind()}.
   */
  public abstract int fixedSize();

  /**
   * Byte order used to decode the raw numeric bytes into a long (or sign-extended int). The default
   * is {@link ByteOrder#LITTLE_ENDIAN}.
   */
  public ByteOrder byteOrder() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  /** The numeric interpretation of the column's bytes. */
  public abstract NumericKind numericKind();

  /**
   * The stored-field variant emitted for this column. The default derives from {@link
   * #numericKind()} — {@code INT→INTEGER}, {@code LONG→LONG}, {@code FLOAT→FLOAT}, {@code
   * DOUBLE→DOUBLE} — so a caller that wants the natural numeric variant does not need to override
   * this method. Override to select {@link StoredValue.Type#BINARY BINARY} (raw bytes) or {@link
   * StoredValue.Type#STRING STRING} (UTF-8) when that is preferable.
   */
  @Override
  public StoredValue.Type storedType() {
    return switch (numericKind()) {
      case INT -> StoredValue.Type.INTEGER;
      case LONG -> StoredValue.Type.LONG;
      case FLOAT -> StoredValue.Type.FLOAT;
      case DOUBLE -> StoredValue.Type.DOUBLE;
    };
  }

  /**
   * Returns a fresh values cursor iterating dense packed values for doc-ids {@code [0, numDocs)},
   * or {@code null} if the column is not dense or does not support bulk iteration. The default
   * implementation returns {@code null}. Each returned {@link org.apache.lucene.util.BytesRef} has
   * length that is a multiple of {@link #fixedSize()}.
   */
  public BinaryValuesCursor values() {
    return null;
  }
}
