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
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/** Similar to SortedSetDocValuesRangeQuery but for a set */
final class SortedSetDocValuesSetQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SortedSetDocValuesSetQuery.class);

  private final String field;
  private final PrefixCodedTerms termData;
  private final int termDataHashCode; // cached hashcode of termData

  /**
   * Creates a new {@link TermInSetQuery} from the given collection of terms. This is most efficient
   * if it is a sorted set or any other collection which has a presorted and distinct spliterator.
   */
  SortedSetDocValuesSetQuery(String field, Collection<BytesRef> terms) {
    this(field, terms.stream());
  }

  /** Creates a new {@link TermInSetQuery} from the given array of terms. */
  SortedSetDocValuesSetQuery(String field, BytesRef... terms) {
    this(field, Arrays.stream(terms));
  }

  private SortedSetDocValuesSetQuery(String field, Stream<BytesRef> stream) {
    this.field = Objects.requireNonNull(field);
    this.termData = stream.sorted().distinct().collect(PrefixCodedTerms.collector(field));
    this.termDataHashCode = this.termData.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(SortedSetDocValuesSetQuery other) {
    // termData might be heavy to compare so check the hash code first
    return termDataHashCode == other.termDataHashCode && termData.equals(other.termData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), termDataHashCode);
  }

  @Override
  public String toString(String defaultField) {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    TermIterator iterator = termData.iterator();
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (!first) {
        builder.append(' ');
      }
      first = false;
      builder.append(new Term(iterator.field(), term).toString());
    }

    return builder.toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(field)
        + RamUsageEstimator.sizeOfObject(termData);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Weight weight = this;
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            final LongBitSet bits = new LongBitSet(values.getValueCount());
            long maxOrd = -1;
            TermIterator termIterator = termData.iterator();
            for (BytesRef term = termIterator.next(); term != null; term = termIterator.next()) {
              final long ord = values.lookupTerm(term);
              if (ord >= 0) {
                maxOrd = ord;
                bits.set(ord);
              }
            }
            // no terms matched in this segment
            if (maxOrd < 0) {
              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.empty());
            }
            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            final TwoPhaseIterator iterator;
            final long max = maxOrd;
            if (singleton != null) {
              iterator =
                  new TwoPhaseIterator(singleton) {
                    @Override
                    public boolean matches() throws IOException {
                      return bits.get(singleton.ordValue());
                    }

                    @Override
                    public float matchCost() {
                      return 3; // lookup in a bitset
                    }
                  };
            } else {
              iterator =
                  new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                      for (int i = 0; i < values.docValueCount(); i++) {
                        long value = values.nextOrd();
                        if (value > max) {
                          return false; // values are sorted, terminate
                        } else if (bits.get(value)) {
                          return true;
                        }
                      }
                      return false;
                    }

                    @Override
                    public float matchCost() {
                      return 3; // lookup in a bitset
                    }
                  };
            }
            return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
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
}
