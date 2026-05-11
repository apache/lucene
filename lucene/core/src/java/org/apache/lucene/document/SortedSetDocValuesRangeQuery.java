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

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongPredicate;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdRange;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrimarySortAlignable;
import org.apache.lucene.search.PrimarySortAlignables;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

final class SortedSetDocValuesRangeQuery extends Query implements PrimarySortAlignable {

  private final String field;
  private final BytesRef lowerValue;
  private final BytesRef upperValue;
  private final boolean lowerInclusive;
  private final boolean upperInclusive;

  SortedSetDocValuesRangeQuery(
      String field,
      BytesRef lowerValue,
      BytesRef upperValue,
      boolean lowerInclusive,
      boolean upperInclusive) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
    this.lowerInclusive = lowerInclusive && lowerValue != null;
    this.upperInclusive = upperInclusive && upperValue != null;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedSetDocValuesRangeQuery that = (SortedSetDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && Objects.equals(lowerValue, that.lowerValue)
        && Objects.equals(upperValue, that.upperValue)
        && lowerInclusive == that.lowerInclusive
        && upperInclusive == that.upperInclusive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, lowerValue, upperValue, lowerInclusive, upperInclusive);
  }

  private record OrdBounds(long minOrd, long maxOrd) {}

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b.append(lowerInclusive ? "[" : "{")
        .append(lowerValue == null ? "*" : lowerValue)
        .append(" TO ")
        .append(upperValue == null ? "*" : upperValue)
        .append(upperInclusive ? "]" : "}")
        .toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (lowerValue == null && upperValue == null) {
      return new FieldExistsQuery(field);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
        SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ConstantScoreScorerSupplier(score(), scoreMode, context.reader().maxDoc()) {
          @Override
          public DocIdSetIterator iterator(long leadCost) throws IOException {

            OrdBounds ob = ordBounds(values);
            final long minOrd = ob.minOrd();
            final long maxOrd = ob.maxOrd();

            // no terms matched in this segment
            if (minOrd > maxOrd
                || (skipper != null
                    && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
              return DocIdSetIterator.empty();
            }

            // all terms matched in this segment
            if (skipper != null
                && skipper.docCount() == context.reader().maxDoc()
                && skipper.minValue() >= minOrd
                && skipper.maxValue() <= maxOrd) {
              return DocIdSetIterator.all(skipper.docCount());
            }

            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            if (singleton != null) {
              if (skipper != null) {
                final DocIdSetIterator psIterator =
                    getDocIdSetIteratorOrNullForPrimarySort(
                        context.reader(), singleton, skipper, minOrd, maxOrd);
                if (psIterator != null) {
                  return psIterator;
                }
              }
              return TwoPhaseIterator.asDocIdSetIterator(
                  DocValuesRangeIterator.forOrdinalRange(singleton, skipper, minOrd, maxOrd));
            }
            return TwoPhaseIterator.asDocIdSetIterator(
                DocValuesRangeIterator.forOrdinalRange(values, skipper, minOrd, maxOrd));
          }

          @Override
          public long cost() {
            return values.cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  private OrdBounds ordBounds(SortedSetDocValues values) throws IOException {
    final long minOrd;
    if (lowerValue == null) {
      minOrd = 0;
    } else {
      final long ord = values.lookupTerm(lowerValue);
      if (ord < 0) {
        minOrd = -1 - ord;
      } else if (lowerInclusive) {
        minOrd = ord;
      } else {
        minOrd = ord + 1;
      }
    }

    final long maxOrd;
    if (upperValue == null) {
      maxOrd = values.getValueCount() - 1;
    } else {
      final long ord = values.lookupTerm(upperValue);
      if (ord < 0) {
        maxOrd = -2 - ord;
      } else if (upperInclusive) {
        maxOrd = ord;
      } else {
        maxOrd = ord - 1;
      }
    }
    return new OrdBounds(minOrd, maxOrd);
  }

  @Override
  public DocIdRange denseDocIdRangeOrNull(LeafReaderContext context) throws IOException {
    if (!(PrimarySortAlignables.primaryIndexSortField(context, field)
        instanceof SortedSetSortField)) {
      return null;
    }
    if (context.reader().getFieldInfos().fieldInfo(field) == null) {
      return null;
    }
    DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
    if (skipper == null) {
      return null;
    }
    SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
    SortedDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton == null) {
      return null;
    }

    OrdBounds ob = ordBounds(values);
    long minOrd = ob.minOrd();
    long maxOrd = ob.maxOrd();

    if (minOrd > maxOrd || (minOrd > skipper.maxValue() || maxOrd < skipper.minValue())) {
      return new DocIdRange(0, 0);
    }

    if (skipper.docCount() == context.reader().maxDoc()
        && skipper.minValue() >= minOrd
        && skipper.maxValue() <= maxOrd) {
      return new DocIdRange(0, context.reader().maxDoc());
    }

    return denseDocIdRangeFromSortedSetPrimarySortSkipper(
        context.reader(), singleton, skipper, minOrd, maxOrd);
  }

  private DocIdSetIterator getDocIdSetIteratorOrNullForPrimarySort(
      LeafReader reader,
      SortedDocValues sortedDocValues,
      DocValuesSkipper skipper,
      long minOrd,
      long maxOrd)
      throws IOException {
    DocIdRange range =
        denseDocIdRangeFromSortedSetPrimarySortSkipper(
            reader, sortedDocValues, skipper, minOrd, maxOrd);
    if (range == null) {
      return null;
    }
    if (range.isEmpty()) {
      return DocIdSetIterator.empty();
    }
    return DocIdSetIterator.range(range.minDoc(), range.maxDoc());
  }

  /** Same doc-id bounds as {@link #getDocIdSetIteratorOrNullForPrimarySort}'s dense iterator. */
  private DocIdRange denseDocIdRangeFromSortedSetPrimarySortSkipper(
      LeafReader reader,
      SortedDocValues sortedDocValues,
      DocValuesSkipper skipper,
      long minOrd,
      long maxOrd)
      throws IOException {
    if (skipper.docCount() != reader.maxDoc()) {
      return null;
    }
    final Sort indexSort = reader.getMetaData().sort();
    if (indexSort == null
        || indexSort.getSort().length == 0
        || indexSort.getSort()[0].getField().equals(field) == false) {
      return null;
    }

    final int minDocID;
    final int maxDocID;
    if (indexSort.getSort()[0].getReverse()) {
      if (skipper.maxValue() <= maxOrd) {
        minDocID = 0;
      } else {
        skipper.advance(Long.MIN_VALUE, maxOrd);
        minDocID = nextDoc(skipper.minDocID(0), sortedDocValues, l -> l <= maxOrd);
      }
      if (skipper.minValue() >= minOrd) {
        maxDocID = skipper.docCount();
      } else {
        skipper.advance(Long.MIN_VALUE, minOrd);
        maxDocID = nextDoc(skipper.minDocID(0), sortedDocValues, l -> l < minOrd);
      }
    } else {
      if (skipper.minValue() >= minOrd) {
        minDocID = 0;
      } else {
        skipper.advance(minOrd, Long.MAX_VALUE);
        minDocID = nextDoc(skipper.minDocID(0), sortedDocValues, l -> l >= minOrd);
      }
      if (skipper.maxValue() <= maxOrd) {
        maxDocID = skipper.docCount();
      } else {
        skipper.advance(maxOrd, Long.MAX_VALUE);
        maxDocID = nextDoc(skipper.minDocID(0), sortedDocValues, l -> l > maxOrd);
      }
    }
    return new DocIdRange(minDocID, maxDocID);
  }

  private static int nextDoc(int startDoc, SortedDocValues docValues, LongPredicate predicate)
      throws IOException {
    int doc = docValues.docID();
    if (startDoc > doc) {
      doc = docValues.advance(startDoc);
    }
    for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = docValues.nextDoc()) {
      if (predicate.test(docValues.ordValue())) {
        break;
      }
    }
    return doc;
  }
}
