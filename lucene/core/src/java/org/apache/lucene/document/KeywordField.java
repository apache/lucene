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

import java.util.Objects;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Field that indexes a per-document String or {@link BytesRef} into an inverted index for fast
 * filtering, stores values in a columnar fashion using {@link DocValuesType#SORTED_SET} doc values
 * for sorting and faceting, and optionally stores values as stored fields for top-hits retrieval.
 * This field does not support scoring: queries produce constant scores. If you need more
 * fine-grained control you can use {@link StringField}, {@link SortedDocValuesField} or {@link
 * SortedSetDocValuesField}, and {@link StoredField}.
 *
 * <p>This field defines static factory methods for creating common query objects:
 *
 * <ul>
 *   <li>{@link #newExactQuery} for matching a value.
 *   <li>{@link #newSetQuery} for matching any of the values coming from a set.
 *   <li>{@link #newSortField} for matching a value.
 * </ul>
 */
public class KeywordField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();
  private static final FieldType FIELD_TYPE_STORED;

  static {
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.setTokenized(false);
    FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_SET);
    FIELD_TYPE.freeze();

    FIELD_TYPE_STORED = new FieldType(FIELD_TYPE);
    FIELD_TYPE_STORED.setStored(true);
    FIELD_TYPE_STORED.freeze();
  }

  private BytesRef binaryValue;
  private final StoredValue storedValue;

  /**
   * Creates a new KeywordField.
   *
   * @param name field name
   * @param value the BytesRef value
   * @param stored whether to store the field
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public KeywordField(String name, BytesRef value, Store stored) {
    super(name, value, stored == Field.Store.YES ? FIELD_TYPE_STORED : FIELD_TYPE);
    this.binaryValue = value;
    if (stored == Store.YES) {
      storedValue = new StoredValue(value);
    } else {
      storedValue = null;
    }
  }

  /**
   * Creates a new KeywordField from a String value, by indexing its UTF-8 representation.
   *
   * @param name field name
   * @param value the BytesRef value
   * @param stored whether to store the field
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public KeywordField(String name, String value, Store stored) {
    super(name, value, stored == Field.Store.YES ? FIELD_TYPE_STORED : FIELD_TYPE);
    this.binaryValue = new BytesRef(value);
    if (stored == Store.YES) {
      storedValue = new StoredValue(value);
    } else {
      storedValue = null;
    }
  }

  @Override
  public BytesRef binaryValue() {
    return binaryValue;
  }

  @Override
  public InvertableType invertableType() {
    return InvertableType.BINARY;
  }

  @Override
  public void setStringValue(String value) {
    super.setStringValue(value);
    binaryValue = new BytesRef(value);
    if (storedValue != null) {
      storedValue.setStringValue(value);
    }
  }

  @Override
  public void setBytesValue(BytesRef value) {
    super.setBytesValue(value);
    binaryValue = value;
    if (storedValue != null) {
      storedValue.setBinaryValue(value);
    }
  }

  @Override
  public StoredValue storedValue() {
    return storedValue;
  }

  /**
   * Create a query for matching an exact {@link BytesRef} value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws NullPointerException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, BytesRef value) {
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(value, "value must not be null");
    return new ConstantScoreQuery(new TermQuery(new Term(field, value)));
  }

  /**
   * Create a query for matching an exact {@link String} value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws NullPointerException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, String value) {
    Objects.requireNonNull(value, "value must not be null");
    return newExactQuery(field, new BytesRef(value));
  }

  /**
   * Create a query for matching any of a set of provided {@link BytesRef} values.
   *
   * @param field field name. must not be {@code null}.
   * @param values the set of values to match
   * @throws NullPointerException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newSetQuery(String field, BytesRef... values) {
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(values, "values must not be null");
    Query indexQuery = new TermInSetQuery(field, values);
    Query dvQuery = new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, field, values);
    return new IndexOrDocValuesQuery(indexQuery, dvQuery);
  }

  /**
   * Create a new {@link SortField} for {@link BytesRef} values.
   *
   * @param field field name. must not be {@code null}.
   * @param reverse true if natural order should be reversed.
   * @param selector custom selector type for choosing the sort value from the set.
   */
  public static SortField newSortField(
      String field, boolean reverse, SortedSetSelector.Type selector) {
    Objects.requireNonNull(field, "field must not be null");
    Objects.requireNonNull(selector, "selector must not be null");
    return new SortedSetSortField(field, reverse, selector);
  }
}
