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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

final class FilteredOnPrimaryIndexSortFieldQuery extends Query {
  private final Query query;
  private final Query filter;
  private final Query fallbackQuery;

  FilteredOnPrimaryIndexSortFieldQuery(Query query, Query filter, Query fallbackQuery) {
    this.query = Objects.requireNonNull(query);
    this.filter = Objects.requireNonNull(filter);
    this.fallbackQuery = Objects.requireNonNull(fallbackQuery);
  }

  static boolean canOptimize(Query filter, IndexSearcher searcher) throws IOException {
    String field = filterField(filter);
    if (field == null) {
      return false;
    }
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      if (filter instanceof IndexSortSortedNumericDocValuesRangeQuery
          && primaryIndexSortField(context, field) != null) {
        return true;
      } else if (filter instanceof TermQuery
          && primaryIndexSortField(context, field) instanceof SortedSetSortField
          && DocValues.unwrapSingleton(DocValues.getSortedSet(context.reader(), field)) != null) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight queryWeight = searcher.createWeight(query, scoreMode, boost);
    final Weight fallbackWeight = searcher.createWeight(fallbackQuery, scoreMode, boost);

    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return fallbackWeight.explain(context, doc);
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return fallbackWeight.matches(context, doc);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        return fallbackWeight.count(context);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier fallbackSupplier = fallbackWeight.scorerSupplier(context);
        if (fallbackSupplier == null) {
          return null;
        }
        final ScorerSupplier querySupplier = queryWeight.scorerSupplier(context);
        if (querySupplier == null) {
          return fallbackSupplier;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return fallbackSupplier.get(leadCost);
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            final DocIdRange range = getDocIdRange(context);
            if (range == null) {
              return fallbackSupplier.bulkScorer();
            }
            if (range.isEmpty()) {
              return emptyBulkScorer();
            }
            return new RangeFilteredBulkScorer(querySupplier.bulkScorer(), range);
          }

          @Override
          public long cost() {
            return fallbackSupplier.cost();
          }

          @Override
          public void setTopLevelScoringClause() {
            fallbackSupplier.setTopLevelScoringClause();
            querySupplier.setTopLevelScoringClause();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return queryWeight.isCacheable(ctx) && fallbackWeight.isCacheable(ctx);
      }
    };
  }

  private DocIdRange getDocIdRange(LeafReaderContext context) throws IOException {
    if (filter instanceof IndexSortSortedNumericDocValuesRangeQuery rangeQuery) {
      return rangeQuery.getDenseDocIdRangeForPrimarySort(context);
    } else if (filter instanceof TermQuery termQuery) {
      return getTermDocIdRange(context, termQuery.getTerm());
    }
    return null;
  }

  private static DocIdRange getTermDocIdRange(LeafReaderContext context, Term term)
      throws IOException {
    if (primaryIndexSortField(context, term.field()) instanceof SortedSetSortField == false) {
      return null;
    }
    SortedSetDocValues values = DocValues.getSortedSet(context.reader(), term.field());
    SortedDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton == null) {
      return null;
    }
    int docFreq = context.reader().docFreq(term);
    long ord = values.lookupTerm(term.bytes());
    if (ord < 0) {
      return docFreq == 0 ? new DocIdRange(0, 0) : null;
    }
    DocIdRange range = getDocIdRangeForPrimarySort(context, term.bytes());
    if (range.isEmpty()) {
      return range;
    }
    if (docFreq != range.maxDoc() - range.minDoc()) {
      return null;
    }
    PostingsEnum postings = context.reader().postings(term, PostingsEnum.NONE);
    if (postings == null || postings.nextDoc() != range.minDoc()) {
      return null;
    }
    if (postings.advance(range.maxDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      return null;
    }
    return range;
  }

  private static SortField primaryIndexSortField(LeafReaderContext context, String field) {
    Sort indexSort = context.reader().getMetaData().sort();
    if (indexSort != null
        && indexSort.getSort().length > 0
        && indexSort.getSort()[0].getField().equals(field)) {
      return indexSort.getSort()[0];
    }
    return null;
  }

  private static String filterField(Query filter) {
    if (filter instanceof IndexSortSortedNumericDocValuesRangeQuery rangeQuery) {
      return rangeQuery.getField();
    } else if (filter instanceof NumericDocValuesRangeQuery rangeQuery) {
      return rangeQuery.getField();
    } else if (filter instanceof PointRangeQuery pointRangeQuery
        && pointRangeQuery.getNumDims() == 1) {
      return pointRangeQuery.getField();
    } else if (filter instanceof TermQuery termQuery) {
      return termQuery.getTerm().field();
    }
    return null;
  }

  private static DocIdRange getDocIdRangeForPrimarySort(
      LeafReaderContext context, BytesRef value) throws IOException {
    SortField sortField = context.reader().getMetaData().sort().getSort()[0];
    int maxDoc = context.reader().maxDoc();

    ValueComparator comparator = loadComparator(sortField, value, context);
    int low = 0;
    int high = maxDoc - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) <= 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, value, context);
      } else {
        low = mid + 1;
      }
    }
    int firstDocIdInclusive = high + 1;

    comparator = loadComparator(sortField, value, context);
    low = firstDocIdInclusive;
    high = maxDoc - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) < 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, value, context);
      } else {
        low = mid + 1;
      }
    }
    int lastDocIdExclusive = high + 1;

    return new DocIdRange(firstDocIdInclusive, lastDocIdExclusive);
  }

  private interface ValueComparator {
    int compare(int docID) throws IOException;
  }

  private static ValueComparator loadComparator(
      SortField sortField, BytesRef topValue, LeafReaderContext context) throws IOException {
    @SuppressWarnings("unchecked")
    FieldComparator<BytesRef> fieldComparator =
        (FieldComparator<BytesRef>) sortField.getComparator(1, Pruning.NONE);
    fieldComparator.setTopValue(topValue);
    LeafFieldComparator leafFieldComparator = fieldComparator.getLeafComparator(context);
    int direction = sortField.getReverse() ? -1 : 1;

    return doc -> direction * leafFieldComparator.compareTop(doc);
  }

  private static BulkScorer emptyBulkScorer() {
    return new BulkScorer() {
      @Override
      public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  private static final class RangeFilteredBulkScorer extends BulkScorer {
    private final BulkScorer in;
    private final DocIdRange range;

    private RangeFilteredBulkScorer(BulkScorer in, DocIdRange range) {
      this.in = Objects.requireNonNull(in);
      this.range = Objects.requireNonNull(range);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      final int filteredMin = Math.max(min, range.minDoc());
      final int filteredMax = Math.min(max, range.maxDoc());
      if (filteredMin >= filteredMax) {
        return max <= range.minDoc() ? range.minDoc() : DocIdSetIterator.NO_MORE_DOCS;
      }

      final int next = in.score(collector, acceptDocs, filteredMin, filteredMax);
      if (range.maxDoc() <= max) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }
      return next;
    }

    @Override
    public long cost() {
      return Math.min(in.cost(), range.maxDoc() - range.minDoc());
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    fallbackQuery.visit(visitor);
  }

  @Override
  public String toString(String field) {
    return "FilteredOnPrimaryIndexSortFieldQuery(query="
        + query.toString(field)
        + ", filter="
        + filter.toString(field)
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    FilteredOnPrimaryIndexSortFieldQuery that = (FilteredOnPrimaryIndexSortFieldQuery) obj;
    return query.equals(that.query)
        && filter.equals(that.filter)
        && fallbackQuery.equals(that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), query, filter, fallbackQuery);
  }
}
