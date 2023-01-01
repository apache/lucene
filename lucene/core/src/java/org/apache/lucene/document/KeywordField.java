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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Field that indexes a per-document {@link BytesRef} into an inverted index for fast filtering and
 * stores values in a columnar fashion using {@link DocValuesType#SORTED_SET} doc values for sorting
 * and faceting. This field does not support scoring: queries produce constant scores. If you also
 * need to store the value, you should add a separate {@link StoredField} instance. If you need more
 * fine-grained control you can use {@link StringField} and {@link SortedDocValuesField} or {@link
 * SortedSetDocValuesField}.
 *
 * <p>This field defines static factory methods for creating common query objects:
 *
 * <ul>
 *   <li>{@link #newExactQuery} for matching a value.
 *   <li>{@link #newSortField} for matching a value.
 * </ul>
 */
public class KeywordField extends Field {

  private static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.setTokenized(false);
    FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_SET);
    FIELD_TYPE.freeze();
  }

  /**
   * Creates a new KeywordField.
   *
   * @param name field name
   * @param value the BytesRef value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public KeywordField(String name, BytesRef value) {
    super(name, value, FIELD_TYPE);
  }

  /**
   * Creates a new KeywordField from a String value, by indexing its UTF-8 representation.
   *
   * @param name field name
   * @param value the BytesRef value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public KeywordField(String name, String value) {
    super(name, value, FIELD_TYPE);
  }

  @Override
  public BytesRef binaryValue() {
    BytesRef binaryValue = super.binaryValue();
    if (binaryValue != null) {
      return binaryValue;
    } else {
      return new BytesRef(stringValue());
    }
  }

  /**
   * Create a query for matching an exact {@link BytesRef} value.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
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
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, String value) {
    return newExactQuery(field, new BytesRef(value));
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
    return new SortedSetSortField(field, reverse, selector);
  }
}
