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
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.Bits;

/**
 * This is a version of knn vector query that provides a query seed to initiate the vector search.
 * NOTE: The underlying format is free to ignore the provided seed
 *
 * <p>See <a href="https://dl.acm.org/doi/10.1145/3539618.3591715">"Lexically-Accelerated Dense
 * Retrieval"</a> (Kulkarni, Hrishikesh and MacAvaney, Sean and Goharian, Nazli and Frieder, Ophir).
 * In SIGIR '23: Proceedings of the 46th International ACM SIGIR Conference on Research and
 * Development in Information Retrieval Pages 152 - 162
 *
 * @lucene.experimental
 */
public class SeededKnnVectorQuery extends AbstractKnnVectorQuery {
  final Query seed;
  final Weight seedWeight;
  final AbstractKnnVectorQuery delegate;

  /**
   * Construct a new SeededKnnVectorQuery instance for a float vector field
   *
   * @param knnQuery the knn query to be seeded
   * @param seed a query seed to initiate the vector format search
   * @return a new SeededKnnVectorQuery instance
   * @lucene.experimental
   */
  public static SeededKnnVectorQuery fromFloatQuery(KnnFloatVectorQuery knnQuery, Query seed) {
    return new SeededKnnVectorQuery(knnQuery, seed, null);
  }

  /**
   * Construct a new SeededKnnVectorQuery instance for a byte vector field
   *
   * @param knnQuery the knn query to be seeded
   * @param seed a query seed to initiate the vector format search
   * @return a new SeededKnnVectorQuery instance
   * @lucene.experimental
   */
  public static SeededKnnVectorQuery fromByteQuery(KnnByteVectorQuery knnQuery, Query seed) {
    return new SeededKnnVectorQuery(knnQuery, seed, null);
  }

  SeededKnnVectorQuery(
      AbstractKnnVectorQuery knnQuery,
      Query seed,
      Weight seedWeight,
      String field,
      int k,
      Query filter,
      KnnSearchStrategy searchStrategy) {
    super(field, k, filter, searchStrategy);
    this.delegate = knnQuery;
    this.seed = Objects.requireNonNull(seed);
    this.seedWeight = seedWeight;
  }

  public SeededKnnVectorQuery(KnnFloatVectorQuery knnQuery, Query seed, Weight seedWeight) {
    this(
        knnQuery,
        seed,
        seedWeight,
        knnQuery.field,
        knnQuery.k,
        knnQuery.filter,
        knnQuery.searchStrategy);
  }

  public SeededKnnVectorQuery(KnnByteVectorQuery knnQuery, Query seed, Weight seedWeight) {
    this(
        knnQuery,
        seed,
        seedWeight,
        knnQuery.field,
        knnQuery.k,
        knnQuery.filter,
        knnQuery.searchStrategy);
  }

  @Override
  public String toString(String field) {
    return "SeededKnnVectorQuery{"
        + "seed="
        + seed
        + ", seedWeight="
        + seedWeight
        + ", delegate="
        + delegate
        + '}';
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (seedWeight != null) {
      return super.rewrite(indexSearcher);
    }
    SeededKnnVectorQuery rewritten =
        new SeededKnnVectorQuery(
            delegate,
            seed,
            createSeedWeight(indexSearcher),
            delegate.field,
            delegate.k,
            delegate.filter,
            delegate.searchStrategy);
    return rewritten.rewrite(indexSearcher);
  }

  Weight createSeedWeight(IndexSearcher indexSearcher) throws IOException {
    BooleanQuery.Builder booleanSeedQueryBuilder =
        new BooleanQuery.Builder()
            .add(seed, BooleanClause.Occur.MUST)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER);
    if (filter != null) {
      booleanSeedQueryBuilder.add(filter, BooleanClause.Occur.FILTER);
    }
    Query seedRewritten = indexSearcher.rewrite(booleanSeedQueryBuilder.build());
    return indexSearcher.createWeight(seedRewritten, ScoreMode.TOP_SCORES, 1f);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      Bits acceptDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    return delegate.approximateSearch(
        context, acceptDocs, visitedLimit, new SeededCollectorManager(knnCollectorManager));
  }

  @Override
  protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
    return delegate.getKnnCollectorManager(k, searcher);
  }

  @Override
  protected TopDocs exactSearch(
      LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout)
      throws IOException {
    return delegate.exactSearch(context, acceptIterator, queryTimeout);
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return delegate.mergeLeafResults(perLeafResults);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    delegate.visit(visitor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SeededKnnVectorQuery that = (SeededKnnVectorQuery) o;
    return Objects.equals(seed, that.seed)
        && Objects.equals(seedWeight, that.seedWeight)
        && Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), seed, seedWeight, delegate);
  }

  @Override
  public String getField() {
    return delegate.getField();
  }

  @Override
  public int getK() {
    return delegate.getK();
  }

  @Override
  public Query getFilter() {
    return delegate.getFilter();
  }

  @Override
  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    return delegate.createVectorScorer(context, fi);
  }

  static class MappedDISI extends DocIdSetIterator {
    KnnVectorValues.DocIndexIterator indexedDISI;
    DocIdSetIterator sourceDISI;

    MappedDISI(KnnVectorValues.DocIndexIterator indexedDISI, DocIdSetIterator sourceDISI) {
      this.indexedDISI = indexedDISI;
      this.sourceDISI = sourceDISI;
    }

    /**
     * Advances the source iterator to the first document number that is greater than or equal to
     * the provided target and returns the corresponding index.
     */
    @Override
    public int advance(int target) throws IOException {
      int newTarget = sourceDISI.advance(target);
      if (newTarget != NO_MORE_DOCS) {
        indexedDISI.advance(newTarget);
      }
      return docID();
    }

    @Override
    public long cost() {
      return sourceDISI.cost();
    }

    @Override
    public int docID() {
      if (indexedDISI.docID() == NO_MORE_DOCS || sourceDISI.docID() == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      return indexedDISI.index();
    }

    /** Advances to the next document in the source iterator and returns the corresponding index. */
    @Override
    public int nextDoc() throws IOException {
      int newTarget = sourceDISI.nextDoc();
      if (newTarget != NO_MORE_DOCS) {
        indexedDISI.advance(newTarget);
      }
      return docID();
    }
  }

  static class TopDocsDISI extends DocIdSetIterator {
    private final int[] sortedDocIds;
    private int idx = -1;

    TopDocsDISI(TopDocs topDocs, LeafReaderContext ctx) {
      sortedDocIds = new int[topDocs.scoreDocs.length];
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        // Remove the doc base as added by the collector
        sortedDocIds[i] = topDocs.scoreDocs[i].doc - ctx.docBase;
      }
      Arrays.sort(sortedDocIds);
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return sortedDocIds.length;
    }

    @Override
    public int docID() {
      if (idx == -1) {
        return -1;
      } else if (idx >= sortedDocIds.length) {
        return DocIdSetIterator.NO_MORE_DOCS;
      } else {
        return sortedDocIds[idx];
      }
    }

    @Override
    public int nextDoc() {
      idx += 1;
      return docID();
    }
  }

  class SeededCollectorManager implements KnnCollectorManager {
    final KnnCollectorManager knnCollectorManager;

    SeededCollectorManager(KnnCollectorManager knnCollectorManager) {
      this.knnCollectorManager = knnCollectorManager;
    }

    @Override
    public KnnCollector newCollector(
        int visitLimit, KnnSearchStrategy searchStrategy, LeafReaderContext ctx)
        throws IOException {
      TopScoreDocCollector seedCollector =
          new TopScoreDocCollectorManager(k, null, Integer.MAX_VALUE).newCollector();
      final LeafReader leafReader = ctx.reader();
      final LeafCollector leafCollector = seedCollector.getLeafCollector(ctx);
      if (leafCollector != null) {
        try {
          BulkScorer scorer = seedWeight.bulkScorer(ctx);
          if (scorer != null) {
            scorer.score(
                leafCollector,
                leafReader.getLiveDocs(),
                0 /* min */,
                DocIdSetIterator.NO_MORE_DOCS /* max */);
          }
        } catch (
            @SuppressWarnings("unused")
            CollectionTerminatedException e) {
        }
        leafCollector.finish();
      }
      KnnCollector delegateCollector =
          knnCollectorManager.newCollector(visitLimit, searchStrategy, ctx);
      TopDocs seedTopDocs = seedCollector.topDocs();
      VectorScorer scorer =
          delegate.createVectorScorer(ctx, leafReader.getFieldInfos().fieldInfo(field));
      if (seedTopDocs.totalHits.value() == 0 || scorer == null) {
        return delegateCollector;
      }
      DocIdSetIterator vectorIterator = scorer.iterator();
      // Handle sparse
      if (vectorIterator instanceof IndexedDISI indexedDISI) {
        vectorIterator = IndexedDISI.asDocIndexIterator(indexedDISI);
      }
      // Most underlying iterators are indexed, so we can map the seed docs to the vector docs
      if (vectorIterator instanceof KnnVectorValues.DocIndexIterator indexIterator) {
        DocIdSetIterator seedDocs =
            new MappedDISI(indexIterator, new TopDocsDISI(seedTopDocs, ctx));
        return knnCollectorManager.newCollector(
            visitLimit,
            new KnnSearchStrategy.Seeded(seedDocs, seedTopDocs.scoreDocs.length, searchStrategy),
            ctx);
      }
      return delegateCollector;
    }
  }
}
