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
import java.io.UncheckedIOException;
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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

final class SortedSetDocValuesRangeQuery extends Query {

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
        final SortedDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null && skipper != null && isDensePrimarySort(context.reader(), skipper)) {
          return getScorerSupplierFromDensePrimarySort(context, singleton, values, skipper);
        }
        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ConstantScoreScorerSupplier(score(), scoreMode, context.reader().maxDoc()) {
          @Override
          public DocIdSetIterator iterator(long leadCost) throws IOException {

            final long minOrd = minOrd(values);
            final long maxOrd = maxOrd(values);

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

            if (singleton != null) {
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

      private ScorerSupplier getScorerSupplierFromDensePrimarySort(
          LeafReaderContext context,
          SortedDocValues singleton,
          SortedSetDocValues values,
          DocValuesSkipper skipper) {
        final Sort indexSort = context.reader().getMetaData().sort();
        return new ConstantScoreScorerSupplier(score(), scoreMode, context.reader().maxDoc()) {
          int skipperMinDocId = -1, skipperMaxDocId = -1;
          long minOrd, maxOrd;
          boolean skipperMinDocIdExact = false, skipperMaxDocIdExact = false;

          @Override
          public DocIdSetIterator iterator(long leadCost) throws IOException {
            if (skipperMinDocId == -1) {
              computeSkipperDocIds();
            }
            final int minDocID;
            final int maxDocID;
            if (indexSort.getSort()[0].getReverse()) {
              minDocID =
                  skipperMinDocIdExact
                      ? skipperMinDocId
                      : nextDoc(skipperMinDocId, singleton, l -> l <= maxOrd);
              maxDocID =
                  skipperMaxDocIdExact
                      ? skipperMaxDocId
                      : nextDoc(skipperMaxDocId, singleton, l -> l < minOrd);
            } else {
              minDocID =
                  skipperMinDocIdExact
                      ? skipperMinDocId
                      : nextDoc(skipperMinDocId, singleton, l -> l >= minOrd);
              maxDocID =
                  skipperMaxDocIdExact
                      ? skipperMaxDocId
                      : nextDoc(skipperMaxDocId, singleton, l -> l > maxOrd);
            }
            return minDocID == maxDocID
                ? DocIdSetIterator.empty()
                : DocIdSetIterator.range(minDocID, maxDocID);
          }

          @Override
          public long cost() {
            if (skipperMinDocId == -1) {
              try {
                // Similar to PointValues, IOExceptions needs to be caught and rethrown as
                // UncheckedIOException
                computeSkipperDocIds();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
            if (skipperMinDocIdExact && skipperMaxDocIdExact) {
              return skipperMaxDocId - skipperMinDocId;
            }
            // TODO: expose skipper block size here?
            return Math.min(context.reader().maxDoc(), 4096 + skipperMaxDocId - skipperMinDocId);
          }

          private void computeSkipperDocIds() throws IOException {
            minOrd = minOrd(values);
            maxOrd = upperValue != null && upperValue.equals(lowerValue) ? minOrd : maxOrd(values);
            if (minOrd > maxOrd || minOrd > skipper.maxValue() || maxOrd < skipper.minValue()) {
              skipperMinDocId = skipperMaxDocId = DocIdSetIterator.NO_MORE_DOCS;
              skipperMinDocIdExact = skipperMaxDocIdExact = true;
              return;
            }
            if (skipper.minValue() >= minOrd && skipper.maxValue() <= maxOrd) {
              skipperMinDocId = 0;
              skipperMaxDocId = skipper.docCount();
              skipperMinDocIdExact = skipperMaxDocIdExact = true;
              return;
            }
            if (indexSort.getSort()[0].getReverse()) {
              if (skipper.maxValue() <= maxOrd) {
                skipperMinDocId = 0;
                skipperMinDocIdExact = true;
              } else {
                skipper.advance(Long.MIN_VALUE, maxOrd);
                skipperMinDocId = skipper.minDocID(0);
                skipperMinDocIdExact = skipper.maxValue(0) == maxOrd;
              }
              if (skipper.minValue() >= minOrd) {
                skipperMaxDocId = skipper.docCount();
                skipperMaxDocIdExact = true;
              } else {
                skipper.advance(Long.MIN_VALUE, minOrd);
                skipperMaxDocId =
                    skipper.minValue(0) == minOrd ? skipper.maxDocID(0) + 1 : skipper.minDocID(0);
                // we can read the next block, if the maxValue is different to minOrd, then we
                // should be done, we
                // don't need to visit the doc values. But what is more expensive, visit one doc
                // value or one skipper block?
                skipperMaxDocIdExact = false;
              }
            } else {
              if (skipper.minValue() >= minOrd) {
                skipperMinDocId = 0;
                skipperMinDocIdExact = true;
              } else {
                skipper.advance(minOrd, Long.MAX_VALUE);
                skipperMinDocId = skipper.minDocID(0);
                skipperMinDocIdExact = skipper.minValue(0) == minOrd;
              }
              if (skipper.maxValue() <= maxOrd) {
                skipperMaxDocId = skipper.docCount();
                skipperMaxDocIdExact = true;
              } else {
                skipper.advance(maxOrd, Long.MAX_VALUE);
                skipperMaxDocId =
                    skipper.maxValue(0) == maxOrd ? skipper.maxDocID(0) + 1 : skipper.minDocID(0);
                // we can read the next block, if the minValue is different to maxOrd, then we
                // should be done, we
                // don't need to visit the doc values. But what is more expensive, visit one doc
                // value or one skipper block?
                skipperMaxDocIdExact = false;
              }
            }
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  private long minOrd(SortedSetDocValues values) throws IOException {
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
    return minOrd;
  }

  private long maxOrd(SortedSetDocValues values) throws IOException {
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
    return maxOrd;
  }

  private boolean isDensePrimarySort(LeafReader reader, DocValuesSkipper skipper) {
    if (skipper.docCount() != reader.maxDoc()) {
      return false;
    }
    final Sort indexSort = reader.getMetaData().sort();
    if (indexSort == null
        || indexSort.getSort().length == 0
        || indexSort.getSort()[0].getField().equals(field) == false) {
      return false;
    }
    return true;
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
