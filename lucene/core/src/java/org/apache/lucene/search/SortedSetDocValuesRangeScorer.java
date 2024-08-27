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
package org.apache.lucene.search;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;
import java.util.Collection;
import java.util.function.LongPredicate;

public class SortedSetDocValuesRangeScorer extends Scorer {
  final Scorer delegate;

  public SortedSetDocValuesRangeScorer(
      String field,
      SortedSetDocValues values,
      long minOrd,
      long maxOrd,
      ScoreMode scoreMode,
      float score,
      DocValuesSkipper skipper,
      LeafReaderContext context) throws IOException {
    delegate = setupScorer(field, values, minOrd, maxOrd, scoreMode, score, skipper, context);
  }

  static Scorer setupScorer(
      String field,
      SortedSetDocValues values,
      long minOrd,
      long maxOrd,
      ScoreMode scoreMode,
      float score,
      DocValuesSkipper skipper,
      LeafReaderContext context) throws IOException {
    // no terms matched in this segment
    if (minOrd > maxOrd
        || (skipper != null
        && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue()))) {
      return new ConstantScoreScorer(score, scoreMode, DocIdSetIterator.empty());
    }

    // all terms matched in this segment
    if (skipper != null
        && skipper.docCount() == context.reader().maxDoc()
        && skipper.minValue() >= minOrd
        && skipper.maxValue() <= maxOrd) {
      return new ConstantScoreScorer(
          score, scoreMode, DocIdSetIterator.all(skipper.docCount()));
    }

    final SortedDocValues singleton = DocValues.unwrapSingleton(values);
    TwoPhaseIterator iterator;
    if (singleton != null) {
      if (skipper != null) {
        final DocIdSetIterator psIterator =
            getDocIdSetIteratorOrNullForPrimarySort(
                context.reader(), field, singleton, skipper, minOrd, maxOrd);
        if (psIterator != null) {
          return new ConstantScoreScorer(score, scoreMode, psIterator);
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
      iterator = new DocValuesRangeIterator(iterator, skipper, minOrd, maxOrd, false);
    }
    return new ConstantScoreScorer(score, scoreMode, iterator);
  }

  @Override
  public int docID() {
    return delegate.docID();
  }

  @Override
  public DocIdSetIterator iterator() {
    return delegate.iterator();
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return delegate.getMaxScore(upTo);
  }

  @Override
  public float score() throws IOException {
    return delegate.score();
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return delegate.twoPhaseIterator();
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return delegate.advanceShallow(target);
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    return delegate.smoothingScore(docId);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    delegate.setMinCompetitiveScore(minScore);
  }

  @Override
  public Collection<ChildScorable> getChildren() throws IOException {
    return delegate.getChildren();
  }

  private static DocIdSetIterator getDocIdSetIteratorOrNullForPrimarySort(
      LeafReader reader,
      String field,
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
    return minDocID == maxDocID
        ? DocIdSetIterator.empty()
        : DocIdSetIterator.range(minDocID, maxDocID);
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
