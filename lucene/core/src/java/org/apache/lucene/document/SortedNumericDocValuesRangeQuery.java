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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.NumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

final class SortedNumericDocValuesRangeQuery extends NumericDocValuesRangeQuery {

  SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
    super(field, lowerValue, upperValue);
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && lowerValue == that.lowerValue
        && upperValue == that.upperValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, lowerValue, upperValue);
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
    return b.append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new FieldExistsQuery(field);
    }
    if (lowerValue > upperValue) {
      return MatchNoDocsQuery.INSTANCE;
    }
    long globalMin = DocValuesSkipper.globalMinValue(indexSearcher, field);
    long globalMax = DocValuesSkipper.globalMaxValue(indexSearcher, field);
    if (lowerValue > globalMax || upperValue < globalMin) {
      return MatchNoDocsQuery.INSTANCE;
    }
    if (lowerValue <= globalMin
        && upperValue >= globalMax
        && DocValuesSkipper.globalDocCount(indexSearcher, field)
            == indexSearcher.getIndexReader().maxDoc()) {
      return MatchAllDocsQuery.INSTANCE;
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }

        int maxDoc = context.reader().maxDoc();
        int count = docCountIgnoringDeletes(context);
        if (count == 0) {
          return null;
        } else if (count == maxDoc) {
          return ConstantScoreScorerSupplier.matchAll(score(), scoreMode, maxDoc);
        }

        SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
        TwoPhaseIterator iterator;
        if (singleton != null) {
          if (skipper != null) {
            final DocIdSetIterator psIterator =
                getDocIdSetIteratorOrNullForPrimarySort(context.reader(), singleton, skipper);
            if (psIterator != null) {
              return ConstantScoreScorerSupplier.fromIterator(
                  psIterator, score(), scoreMode, maxDoc);
            }
          }
          iterator =
              new TwoPhaseIterator(singleton) {
                @Override
                public boolean matches() throws IOException {
                  final long value = singleton.longValue();
                  return value >= lowerValue && value <= upperValue;
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
                  for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                    final long value = values.nextValue();
                    if (value < lowerValue) {
                      continue;
                    }
                    // Values are sorted, so the first value that is >= lowerValue is our best
                    // candidate
                    return value <= upperValue;
                  }
                  return false; // all values were < lowerValue
                }

                @Override
                public float matchCost() {
                  return 2; // 2 comparisons
                }
              };
        }
        if (skipper != null) {
          iterator = new DocValuesRangeIterator(iterator, skipper, lowerValue, upperValue, false);
        }
        return ConstantScoreScorerSupplier.fromIterator(
            TwoPhaseIterator.asDocIdSetIterator(iterator), score(), scoreMode, maxDoc);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        int maxDoc = context.reader().maxDoc();
        int cnt = docCountIgnoringDeletes(context);
        if (cnt == maxDoc) {
          // Return LeafReader#numDocs that accounts for deleted documents as well
          return context.reader().numDocs();
        }
        return cnt;
      }

      /* Returns
       * # docs within the query range ignoring any deleted documents
       * -1 if # docs cannot be determined efficiently
       */
      private int docCountIgnoringDeletes(LeafReaderContext context) throws IOException {
        final DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
        if (skipper != null) {
          if (skipper.minValue() > upperValue || skipper.maxValue() < lowerValue) {
            return 0;
          }
          if (skipper.docCount() == context.reader().maxDoc()
              && skipper.minValue() >= lowerValue
              && skipper.maxValue() <= upperValue) {
            return context.reader().maxDoc();
          }
        }
        return -1;
      }
    };
  }

  private DocIdSetIterator getDocIdSetIteratorOrNullForPrimarySort(
      LeafReader reader, NumericDocValues numericDocValues, DocValuesSkipper skipper)
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
      if (skipper.maxValue() <= upperValue) {
        minDocID = 0;
      } else {
        skipper.advance(Long.MIN_VALUE, upperValue);
        minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l <= upperValue);
      }
      if (skipper.minValue() >= lowerValue) {
        maxDocID = skipper.docCount();
      } else {
        skipper.advance(Long.MIN_VALUE, lowerValue);
        maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l < lowerValue);
      }
    } else {
      if (skipper.minValue() >= lowerValue) {
        minDocID = 0;
      } else {
        skipper.advance(lowerValue, Long.MAX_VALUE);
        minDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l >= lowerValue);
      }
      if (skipper.maxValue() <= upperValue) {
        maxDocID = skipper.docCount();
      } else {
        skipper.advance(upperValue, Long.MAX_VALUE);
        maxDocID = nextDoc(skipper.minDocID(0), numericDocValues, l -> l > upperValue);
      }
    }
    return minDocID == maxDocID
        ? DocIdSetIterator.empty()
        : DocIdSetIterator.range(minDocID, maxDocID);
  }

  private static int nextDoc(int startDoc, NumericDocValues docValues, LongPredicate predicate)
      throws IOException {
    int doc = docValues.docID();
    if (startDoc > doc) {
      doc = docValues.advance(startDoc);
    }
    for (; doc < DocIdSetIterator.NO_MORE_DOCS; doc = docValues.nextDoc()) {
      if (predicate.test(docValues.longValue())) {
        break;
      }
    }
    return doc;
  }
}
