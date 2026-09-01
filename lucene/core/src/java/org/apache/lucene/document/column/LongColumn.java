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

import java.util.Objects;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableFieldType;

/**
 * A {@link Column} that provides long values. Used for {@link
 * org.apache.lucene.index.DocValuesType#NUMERIC NUMERIC} and {@link
 * org.apache.lucene.index.DocValuesType#SORTED_NUMERIC SORTED_NUMERIC} doc values and for stored
 * numeric fields.
 *
 * <p>Iteration is performed via cursors. {@link #tuples()} is always available and yields {@code
 * (docID, longValue)} pairs. {@link #values()} is a bulk cursor over consecutive doc-ids; it must
 * be overridden when {@link #density()} is {@link Column.Density#DENSE DENSE} and is only consulted
 * in that case.
 *
 * <p>{@link #numericKind()} marks how the long bits should be interpreted. Defaults to {@link
 * NumericKind#LONG LONG}; pass {@link NumericKind#INT INT} (low 32 bits, sign-extended), {@link
 * NumericKind#FLOAT FLOAT} (low 32 bits encoded via {@link
 * org.apache.lucene.util.NumericUtils#floatToSortableInt}), or {@link NumericKind#DOUBLE DOUBLE}
 * (full 64 bits encoded via {@link org.apache.lucene.util.NumericUtils#doubleToSortableLong}) to
 * the constructor to select another interpretation. Callers are responsible for producing the
 * sortable encoding; doc values writes the long unchanged, points consumes it as sortable bytes,
 * and stored fields round-trips it back to {@code float}/{@code double} via {@link
 * org.apache.lucene.util.NumericUtils#sortableIntToFloat} / {@link
 * org.apache.lucene.util.NumericUtils#sortableLongToDouble}. The numeric kind drives the default
 * {@link #storedType()}.
 *
 * @lucene.experimental
 */
public abstract class LongColumn extends Column {

  /** The numeric interpretation of the column's long values. */
  public enum NumericKind {
    /** Type of integer values. */
    INT,
    /** Type of long values. */
    LONG,
    /** Type of float values. */
    FLOAT,
    /** Type of double values. */
    DOUBLE,
  }

  private final NumericKind numericKind;

  /** Creates a LongColumn with {@link NumericKind#LONG}. */
  protected LongColumn(String name, IndexableFieldType fieldType, Density density) {
    this(name, fieldType, density, NumericKind.LONG);
  }

  /** Creates a LongColumn with the given numeric interpretation. */
  protected LongColumn(
      String name, IndexableFieldType fieldType, Density density, NumericKind numericKind) {
    super(name, fieldType, density);
    this.numericKind = Objects.requireNonNull(numericKind, "numericKind must not be null");
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract LongTupleCursor tuples();

  /**
   * Returns a fresh values cursor iterating dense long values for doc-ids {@code [0, numDocs)}.
   * Must be overridden when {@link Column#density()} is {@link Column.Density#DENSE DENSE}; the
   * default implementation throws {@link UnsupportedOperationException} and is never called for
   * {@link Column.Density#SPARSE SPARSE} columns.
   */
  public LongValuesCursor values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }

  /** The numeric interpretation of the column's long values. */
  public final NumericKind numericKind() {
    return numericKind;
  }

  /**
   * The stored-field variant emitted for this column. The default derives from {@link
   * #numericKind()} — {@code INT→INTEGER}, {@code LONG→LONG}, {@code FLOAT→FLOAT}, {@code
   * DOUBLE→DOUBLE} — so a caller that wants the natural numeric variant does not need to override
   * this method. Only numeric {@link org.apache.lucene.document.StoredValue.Type} values are
   * permitted; non-numeric stored data should use a {@link BinaryColumn}.
   */
  public final StoredValue.Type storedType() {
    return switch (numericKind) {
      case INT -> StoredValue.Type.INTEGER;
      case LONG -> StoredValue.Type.LONG;
      case FLOAT -> StoredValue.Type.FLOAT;
      case DOUBLE -> StoredValue.Type.DOUBLE;
    };
  }
}
