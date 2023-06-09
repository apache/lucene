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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Field that stores a per-document <code>int</code> value for scoring, sorting or value retrieval
 * and index the field for fast range filters. If you need more fine-grained control you can use
 * {@link IntPoint}, {@link NumericDocValuesField} or {@link SortedNumericDocValuesField}, and
 * {@link StoredField}.
 *
 * <p>This field defines static factory methods for creating common queries:
 *
 * <ul>
 *   <li>{@link #newExactQuery(String, int)} for matching an exact 1D point.
 *   <li>{@link #newRangeQuery(String, int, int)} for matching a 1D range.
 *   <li>{@link #newSetQuery(String, int...)} for matching a 1D set.
 * </ul>
 *
 * @see PointValues
 */
public final class IntField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();
  private static final FieldType FIELD_TYPE_STORED;

  static {
    FIELD_TYPE.setDimensions(1, Integer.BYTES);
    FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    FIELD_TYPE.freeze();

    FIELD_TYPE_STORED = new FieldType(FIELD_TYPE);
    FIELD_TYPE_STORED.setStored(true);
    FIELD_TYPE_STORED.freeze();
  }

  private final StoredValue storedValue;

  /**
   * Creates a new IntField, indexing the provided point and storing it as a DocValue.
   *
   * @param name field name
   * @param value the int value
   * @throws IllegalArgumentException if the field name or value is null.
   * @deprecated Use {@link #IntField(String, int, Field.Store)} with {@link Field.Store#NO}
   *     instead.
   */
  @Deprecated
  public IntField(String name, int value) {
    this(name, value, Field.Store.NO);
  }

  /**
   * Creates a new IntField, indexing the provided point, storing it as a DocValue, and optionally
   * storing it as a stored field.
   *
   * @param name field name
   * @param value the int value
   * @param stored whether to store the field
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public IntField(String name, int value, Field.Store stored) {
    super(name, stored == Field.Store.YES ? FIELD_TYPE_STORED : FIELD_TYPE);
    fieldsData = value;
    if (stored == Field.Store.YES) {
      storedValue = new StoredValue(value);
    } else {
      storedValue = null;
    }
  }

  @Override
  public BytesRef binaryValue() {
    var bytes = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes((Integer) fieldsData, bytes, 0);
    return new BytesRef(bytes);
  }

  @Override
  public StoredValue storedValue() {
    return storedValue;
  }

  @Override
  public void setIntValue(int value) {
    super.setIntValue(value);
    if (storedValue != null) {
      storedValue.setIntValue(value);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
  }

  /**
   * Create a query for matching an exact integer value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, int value) {
    return newRangeQuery(field, value, value);
  }

  /**
   * Create a range query for integer values.
   *
   * <p>You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries) by setting
   * {@code lowerValue = Integer.MIN_VALUE} or {@code upperValue = Integer.MAX_VALUE}.
   *
   * <p>Ranges are inclusive. For exclusive ranges, pass {@code Math.addExact(lowerValue, 1)} or
   * {@code Math.addExact(upperValue, -1)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive).
   * @param upperValue upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, int lowerValue, int upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    Query fallbackQuery =
        new IndexOrDocValuesQuery(
            IntPoint.newRangeQuery(field, lowerValue, upperValue),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue));
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  /**
   * Create a query matching values in a supplied set
   *
   * @param field field name. must not be {@code null}.
   * @param values integer values
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this set.
   */
  public static Query newSetQuery(String field, int... values) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    long points[] = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      points[i] = values[i];
    }
    return new IndexOrDocValuesQuery(
        IntPoint.newSetQuery(field, values.clone()),
        SortedNumericDocValuesField.newSlowSetQuery(field, points));
  }

  /**
   * Create a new {@link SortField} for int values.
   *
   * @param field field name. must not be {@code null}.
   * @param reverse true if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   */
  public static SortField newSortField(
      String field, boolean reverse, SortedNumericSelector.Type selector) {
    return new SortedNumericSortField(field, SortField.Type.INT, reverse, selector);
  }
}
