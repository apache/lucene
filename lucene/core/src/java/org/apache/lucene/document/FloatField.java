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
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Field that stores a per-document <code>float</code> value for scoring, sorting or value retrieval
 * and index the field for fast range filters. If you also need to store the value, you should add a
 * separate {@link StoredField} instance. If you need more fine-grained control you can use {@link
 * FloatPoint} and {@link FloatDocValuesField}.
 *
 * <p>This field defines static factory methods for creating common queries:
 *
 * <ul>
 *   <li>{@link #newExactQuery(String, float)} for matching an exact 1D point.
 *   <li>{@link #newRangeQuery(String, float, float)} for matching a 1D range.
 *   <li>{@link #newSetQuery(String, float...)} for matching a 1D set.
 * </ul>
 *
 * @see PointValues
 */
public final class FloatField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setDimensions(1, Float.BYTES);
    FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    FIELD_TYPE.freeze();
  }

  /**
   * Creates a new FloatField, indexing the provided point and storing it as a DocValue
   *
   * @param name field name
   * @param value the float value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public FloatField(String name, float value) {
    super(name, FIELD_TYPE);
    fieldsData = (long) NumericUtils.floatToSortableInt(value);
  }

  @Override
  public BytesRef binaryValue() {
    byte[] encodedPoint = new byte[Float.BYTES];
    float value = getValueAsFloat();
    FloatPoint.encodeDimension(value, encodedPoint, 0);
    return new BytesRef(encodedPoint);
  }

  private float getValueAsFloat() {
    return NumericUtils.sortableIntToFloat(numericValue().intValue());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " <" + name + ':' + getValueAsFloat() + '>';
  }

  @Override
  public void setFloatValue(float value) {
    super.setLongValue(NumericUtils.floatToSortableInt(value));
  }

  @Override
  public void setLongValue(long value) {
    throw new IllegalArgumentException("cannot change value type from Float to Long");
  }

  /**
   * Create a query for matching an exact float value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, float value) {
    return newRangeQuery(field, value, value);
  }

  /**
   * Create a range query for float values.
   *
   * <p>You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries) by setting
   * {@code lowerValue = Float.NEGATIVE_INFINITY} or {@code upperValue = Float.POSITIVE_INFINITY}.
   *
   * <p>Range comparisons are consistent with {@link Float#compareTo(Float)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive).
   * @param upperValue upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, float lowerValue, float upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new IndexOrDocValuesQuery(
        FloatPoint.newRangeQuery(field, lowerValue, upperValue),
        SortedNumericDocValuesField.newSlowRangeQuery(
            field,
            NumericUtils.floatToSortableInt(lowerValue),
            NumericUtils.floatToSortableInt(upperValue)));
  }

  /**
   * Create a query matching values in a supplied set
   *
   * @param field field name. must not be {@code null}.
   * @param values float values
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this set.
   */
  public static Query newSetQuery(String field, float... values) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    long points[] = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      points[i] = NumericUtils.floatToSortableInt(values[i]);
    }
    return new IndexOrDocValuesQuery(
        FloatPoint.newSetQuery(field, values.clone()),
        SortedNumericDocValuesField.newSlowSetQuery(field, points));
  }

  /**
   * Create a new {@link SortField} for float values.
   *
   * @param field field name. must not be {@code null}.
   * @param reverse true if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   */
  public static SortField newSortField(
      String field, boolean reverse, SortedNumericSelector.Type selector) {
    return new SortedNumericSortField(field, SortField.Type.FLOAT, reverse, selector);
  }
}
