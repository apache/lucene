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
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
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

        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {

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

            // no terms matched in this segment
            if (minOrd > maxOrd
                || (skipper != null
                    && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
              return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
            }

            // all terms matched in this segment
            if (skipper != null
                && skipper.docCount() == context.reader().maxDoc()
                && skipper.minValue() >= minOrd
                && skipper.maxValue() <= maxOrd) {
              return new ConstantScoreScorer(
                  score(), scoreMode, DocIdSetIterator.all(skipper.docCount()));
            }

            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            TwoPhaseIterator iterator;
            if (singleton != null) {
              if (skipper != null) {
                final DocIdSetIterator psIterator =
                    getDocIdSetIteratorOrNullForPrimarySort(
                        context.reader(), singleton, skipper, minOrd, maxOrd);
                if (psIterator != null) {
                  return new ConstantScoreScorer(score(), scoreMode, psIterator);
                }
              }
              iterator =
                  new TwoPhaseIterator(singleton) {
                    @Override
                    public boolean matches() throws IOException {
                      final long ord = singleton.ordValue();
                      return ord >= minOrd && ord <= maxOrd;
                    }

                    @Override
                    public float matchCost() {
                      return 2; // 2 comparisons
                    }
                  };
            } else {
              iterator =
                  new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                      for (int i = 0; i < values.docValueCount(); i++) {
                        long ord = values.nextOrd();
                        if (ord < minOrd) {
                          continue;
                        }
                        // Values are sorted, so the first ord that is >= minOrd is our best
                        // candidate
                        return ord <= maxOrd;
                      }
                      return false; // all ords were < minOrd
                    }

                    @Override
                    public float matchCost() {
                      return 2; // 2 comparisons
                    }
                  };
            }
            if (skipper != null) {
              iterator = new DocValuesRangeIterator(iterator, skipper, minOrd, maxOrd);
            }
            return new ConstantScoreScorer(score(), scoreMode, iterator);
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

  private DocIdSetIterator getDocIdSetIteratorOrNullForPrimarySort(
      LeafReader reader,
      SortedDocValues sortedDocValues,
      DocValuesSkipper skipper,
      long minOrd,
      long maxOrd)
      throws IOException {
    if (skipper.docCount() != reader.maxDoc()) {
      return null;
    }
    final Sort indexSort = reader.getMetaData().getSort();
    if (indexSort == null
        || indexSort.getSort().length == 0
        || indexSort.getSort()[0].getField().equals(field) == false) {
      return null;
    }

    final boolean reverse = indexSort.getSort()[0].getReverse();
    final int minDocID;
    final int maxDocID;
    skipper.advance(0);
    if (reverse) {
      minDocID =
          skipper.maxValue() <= maxOrd
              ? 0
              : nextDoc(skipper, sortedDocValues, l -> l <= maxOrd, true);
      maxDocID =
          skipper.minValue() >= minOrd
              ? skipper.docCount()
              : nextDoc(skipper, sortedDocValues, l -> l < minOrd, true);
    } else {
      minDocID =
          skipper.minValue() >= minOrd
              ? 0
              : nextDoc(skipper, sortedDocValues, l -> l >= minOrd, false);
      maxDocID =
          skipper.maxValue() <= maxOrd
              ? skipper.docCount()
              : nextDoc(skipper, sortedDocValues, l -> l > maxOrd, false);
    }
    return minDocID == maxDocID
        ? DocIdSetIterator.empty()
        : DocIdSetIterator.range(minDocID, maxDocID);
  }

  private static int nextDoc(
      DocValuesSkipper skipper,
      SortedDocValues docValues,
      LongPredicate competitive,
      boolean reverse)
      throws IOException {
    while (true) {
      if (skipper.minDocID(0) == DocIdSetIterator.NO_MORE_DOCS) {
        return -1; // should not happen
      }
      if (competitive.test(reverse ? skipper.minValue(0) : skipper.maxValue(0))) {
        int doc = docValues.docID();
        if (skipper.minDocID(0) > doc) {
          doc = docValues.advance(skipper.minDocID(0));
        }
        for (; doc <= skipper.maxDocID(0); doc = docValues.nextDoc()) {
          if (competitive.test(docValues.ordValue())) {
            return doc;
          }
        }
      }
      int maxDocID = skipper.maxDocID(0);
      int nextLevel = 1;
      while (nextLevel < skipper.numLevels()
          && competitive.test(reverse ? skipper.minValue(nextLevel) : skipper.maxValue(nextLevel))
              == false) {
        maxDocID = skipper.maxDocID(nextLevel);
        nextLevel++;
      }
      skipper.advance(maxDocID + 1);
    }
  }
}
