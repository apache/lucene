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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
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
 * Field that stores a per-document <code>long</code> value for scoring, sorting or value retrieval
 * and index the field for fast range filters. If you also need to store the value, you should add a
 * separate {@link StoredField} instance. If you need more fine-grained control you can use {@link
 * LongPoint} and {@link NumericDocValuesField} or {@link SortedNumericDocValuesField}.
 *
 * <p>This field defines static factory methods for creating common queries:
 *
 * <ul>
 *   <li>{@link #newExactQuery(String, long)} for matching an exact 1D point.
 *   <li>{@link #newRangeQuery(String, long, long)} for matching a 1D range.
 *   <li>{@link #newSetQuery(String, long...)} for matching a 1D set.
 * </ul>
 *
 * @see PointValues
 */
public final class LongField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setDimensions(1, Long.BYTES);
    FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    FIELD_TYPE.freeze();
  }

  /**
   * Creates a new LongField, indexing the provided point and storing it as a DocValue
   *
   * @param name field name
   * @param value the long value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public LongField(String name, long value) {
    super(name, FIELD_TYPE);
    fieldsData = value;
  }

  @Override
  public BytesRef binaryValue() {
    var bytes = new byte[Long.BYTES];
    NumericUtils.longToSortableBytes((Long) fieldsData, bytes, 0);
    return new BytesRef(bytes);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
  }

  /**
   * Create a query for matching an exact long value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, long value) {
    return newRangeQuery(field, value, value);
  }

  /**
   * Create a range query for long values.
   *
   * <p>You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries) by setting
   * {@code lowerValue = Long.MIN_VALUE} or {@code upperValue = Long.MAX_VALUE}.
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
  public static Query newRangeQuery(String field, long lowerValue, long upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    Query fallbackQuery =
        new IndexOrDocValuesQuery(
            LongPoint.newRangeQuery(field, lowerValue, upperValue),
            SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue));
    return new IndexSortSortedNumericDocValuesRangeQuery(
        field, lowerValue, upperValue, fallbackQuery);
  }

  /**
   * Create a query matching values in a supplied set
   *
   * @param field field name. must not be {@code null}.
   * @param values long values
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this set.
   */
  public static Query newSetQuery(String field, long... values) {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    long points[] = values.clone();
    return new IndexOrDocValuesQuery(
        LongPoint.newSetQuery(field, points),
        SortedNumericDocValuesField.newSlowSetQuery(field, points));
  }

  /**
   * Create a new {@link SortField} for long values.
   *
   * @param field field name. must not be {@code null}.
   * @param reverse true if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   */
  public static SortField newSortField(
      String field, boolean reverse, SortedNumericSelector.Type selector) {
    return new SortedNumericSortField(field, SortField.Type.LONG, reverse, selector);
  }

  /**
   * Returns a query that scores documents based on their distance to {@code origin}: {@code score =
   * weight * pivotDistance / (pivotDistance + distance)}, ie. score is in the {@code [0, weight]}
   * range, is equal to {@code weight} when the document's value is equal to {@code origin} and is
   * equal to {@code weight/2} when the document's value is distant of {@code pivotDistance} from
   * {@code origin}. In case of multi-valued fields, only the closest point to {@code origin} will
   * be considered. This query is typically useful to boost results based on recency by adding this
   * query to a {@link Occur#SHOULD} clause of a {@link BooleanQuery}.
   */
  public static Query newDistanceFeatureQuery(
      String field, float weight, long origin, long pivotDistance) {
    Query query = new LongDistanceFeatureQuery(field, origin, pivotDistance);
    if (weight != 1f) {
      query = new BoostQuery(query, weight);
    }
    return query;
  }
}
