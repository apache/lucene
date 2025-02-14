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
package org.apache.lucene.sandbox.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;

/**
 * A few query builders for doc values multi range queries.
 *
 * @lucene.experimental
 */
public final class DocValuesMultiRangeQuery {

  private DocValuesMultiRangeQuery() {}

  /** Representation of a single clause in a MultiRangeQuery */
  static final class Range {
    BytesRef lower;
    BytesRef upper;

    /** NB: One absolutely must copy ByteRefs before. */
    Range(BytesRef lowerValue, BytesRef upperValue) {
      this.lower = lowerValue;
      this.upper = upperValue;
    }

    // TODO test equals
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Range that = (Range) o;
      return lower.equals(that.lower) && upper.equals(that.upper);
    }

    @Override
    public int hashCode() {
      int result = lower.hashCode();
      result = 31 * result + upper.hashCode();
      return result;
    }
  }

  /**
   * Builder for creating a multi-range query for stabbing by SortedSet or Sorted field values. For
   * example, it matches IPs in docvalues field by multiple IP ranges. For the single range it
   * behaves like {@link SortedSetDocValuesField#newSlowRangeQuery(String, BytesRef, BytesRef,
   * boolean, boolean)} with both true arguments
   */
  public static class SortedSetStabbingBuilder {
    protected final String fieldName;
    final List<Range> clauses = new ArrayList<>();

    public SortedSetStabbingBuilder(String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName);
    }

    // TODO support nulls as min,max boundaries ???
    /** NB:Deeply copies the given bytes */
    public SortedSetStabbingBuilder add(BytesRef lowerValue, BytesRef upperValue) {
      BytesRef lowRef = BytesRef.deepCopyOf(lowerValue);
      BytesRef upRef = BytesRef.deepCopyOf(upperValue);
      clauses.add(new Range(lowRef, upRef));
      return this;
    }

    public Query build() {
      if (clauses.isEmpty()) {
        return new MatchNoDocsQuery();
      }
      if (clauses.size() == 1) {
        Range theOnlyOne = clauses.getFirst();
        return SortedSetDocValuesField.newSlowRangeQuery(
            fieldName, theOnlyOne.lower, theOnlyOne.upper, true, true);
      }
      return createSortedSetDocValuesMultiRangeQuery();
    }

    SortedSetDocValuesMultiRangeQuery createSortedSetDocValuesMultiRangeQuery() {
      return new SortedSetDocValuesMultiRangeQuery(fieldName, clauses);
    }
  }
}
