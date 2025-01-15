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
package org.apache.lucene.search.knn;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.IOFunction;

/**
 * A {@link KnnCollectorManager} that provides seeded knn collection. See usage in {@link
 * org.apache.lucene.search.SeededKnnFloatVectorQuery} and {@link
 * org.apache.lucene.search.SeededKnnByteVectorQuery}.
 */
public class SeededKnnCollectorManager implements KnnCollectorManager {
  private final KnnCollectorManager delegate;
  private final Weight seedWeight;
  private final int k;
  private final IOFunction<LeafReader, KnnVectorValues> vectorValuesSupplier;

  public SeededKnnCollectorManager(
      KnnCollectorManager delegate,
      Weight seedWeight,
      int k,
      IOFunction<LeafReader, KnnVectorValues> vectorValuesSupplier) {
    this.delegate = delegate;
    this.seedWeight = seedWeight;
    this.k = k;
    this.vectorValuesSupplier = vectorValuesSupplier;
  }

  @Override
  public KnnCollector newCollector(int visitedLimit, LeafReaderContext ctx) throws IOException {
    // Execute the seed query
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

    TopDocs seedTopDocs = seedCollector.topDocs();
    KnnVectorValues vectorValues = vectorValuesSupplier.apply(leafReader);
    final KnnCollector delegateCollector = delegate.newCollector(visitedLimit, ctx);
    if (seedTopDocs.totalHits.value() == 0 || vectorValues == null) {
      return delegateCollector;
    }
    KnnVectorValues.DocIndexIterator indexIterator = vectorValues.iterator();
    DocIdSetIterator seedDocs = new MappedDISI(indexIterator, new TopDocsDISI(seedTopDocs));
    return new SeededKnnCollector(delegateCollector, seedDocs, seedTopDocs.scoreDocs.length);
  }

  private static class MappedDISI extends DocIdSetIterator {
    KnnVectorValues.DocIndexIterator indexedDISI;
    DocIdSetIterator sourceDISI;

    private MappedDISI(KnnVectorValues.DocIndexIterator indexedDISI, DocIdSetIterator sourceDISI) {
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

  private static class TopDocsDISI extends DocIdSetIterator {
    private final int[] sortedDocIds;
    private int idx = -1;

    private TopDocsDISI(TopDocs topDocs) {
      sortedDocIds = new int[topDocs.scoreDocs.length];
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
        sortedDocIds[i] = topDocs.scoreDocs[i].doc;
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
}
