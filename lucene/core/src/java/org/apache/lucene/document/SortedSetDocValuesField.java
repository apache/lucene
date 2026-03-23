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

import java.util.Collection;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Field that stores a set of per-document {@link BytesRef} values, indexed for
 * faceting,grouping,joining. Here's an example usage:
 *
 * <pre><code class="language-java">
 *   document.add(new SortedSetDocValuesField(name, new BytesRef("hello")));
 *   document.add(new SortedSetDocValuesField(name, new BytesRef("world")));
 * </code></pre>
 *
 * <p>If you also need to store the value, you should add a separate {@link StoredField} instance.
 *
 * <p>Each value can be at most 32766 bytes long.
 */
public class SortedSetDocValuesField extends Field {

  /** Type for sorted bytes DocValues */
  public static final FieldType TYPE = new FieldType();

  private static final FieldType INDEXED_TYPE;

  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_SET);
    TYPE.freeze();

    INDEXED_TYPE = new FieldType(TYPE);
    INDEXED_TYPE.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
    INDEXED_TYPE.freeze();
  }

  /**
   * Creates a new {@link SortedSetDocValuesField} with the specified 64-bit long value that also
   * creates a {@link FieldType#docValuesSkipIndexType() skip index}.
   *
   * @param name field name
   * @param bytes binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public static SortedSetDocValuesField indexedField(String name, BytesRef bytes) {
    return new SortedSetDocValuesField(name, bytes, INDEXED_TYPE);
  }

  /**
   * Create a new sorted DocValues field.
   *
   * @param name field name
   * @param bytes binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public SortedSetDocValuesField(String name, BytesRef bytes) {
    this(name, bytes, TYPE);
  }

  private SortedSetDocValuesField(String name, BytesRef bytes, FieldType fieldType) {
    super(name, fieldType);
    fieldsData = bytes;
  }

  /**
   * Create a range query that matches all documents whose value is between {@code lowerValue} and
   * {@code upperValue}.
   *
   * <p>This query also works with fields that have indexed {@link SortedDocValuesField}s.
   *
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match, which makes them
   * slow if they are not ANDed with a selective query. As a consequence, they are best used wrapped
   * in an {@link IndexOrDocValuesQuery}, alongside a range query that executes on points, such as
   * {@link BinaryPoint#newRangeQuery}.
   */
  public static Query newSlowRangeQuery(
      String field,
      BytesRef lowerValue,
      BytesRef upperValue,
      boolean lowerInclusive,
      boolean upperInclusive) {
    return new SortedSetDocValuesRangeQuery(
        field, lowerValue, upperValue, lowerInclusive, upperInclusive);
  }

  /**
   * Create a query for matching an exact {@link BytesRef} value.
   *
   * <p>This query also works with fields that have indexed {@link SortedDocValuesField}s.
   *
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match, which makes them
   * slow if they are not ANDed with a selective query. As a consequence, they are best used wrapped
   * in an {@link IndexOrDocValuesQuery}, alongside a range query that executes on points, such as
   * {@link BinaryPoint#newExactQuery}.
   */
  public static Query newSlowExactQuery(String field, BytesRef value) {
    return newSlowRangeQuery(field, value, value, true, true);
  }

  /**
   * Create a query matching any of the specified values.
   *
   * <p>This query also works with fields that have indexed {@link SortedDocValuesField}s.
   *
   * <p><b>NOTE</b>: Such queries cannot efficiently advance to the next match, which makes them
   * slow if they are not ANDed with a selective query. As a consequence, they are best used wrapped
   * in an {@link IndexOrDocValuesQuery}, alongside a set query that executes on postings, such as
   * {@link TermInSetQuery}.
   */
  public static Query newSlowSetQuery(String field, Collection<BytesRef> values) {
    return new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, field, values);
  }
}
