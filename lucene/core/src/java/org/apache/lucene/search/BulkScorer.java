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
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * This class is used to score a range of documents at once, and is returned by {@link
 * Weight#bulkScorer}. Only queries that have a more optimized means of scoring across a range of
 * documents need to override this. Otherwise, a default implementation is wrapped around the {@link
 * Scorer} returned by {@link Weight#scorer}.
 */
public abstract class BulkScorer {

  /**
   * Collects matching documents in a range and return an estimation of the next matching document
   * which is on or after {@code max}.
   *
   * <p>The return value must be:
   *
   * <ul>
   *   <li>&gt;= {@code max},
   *   <li>{@link DocIdSetIterator#NO_MORE_DOCS} if there are no more matches,
   *   <li>&lt;= the first matching document that is &gt;= {@code max} otherwise.
   * </ul>
   *
   * <p>{@code min} is the minimum document to be considered for matching. All documents strictly
   * before this value must be ignored.
   *
   * <p>Although {@code max} would be a legal return value for this method, higher values might help
   * callers skip more efficiently over non-matching portions of the docID space.
   *
   * <p>For instance, a {@link Scorer}-based implementation could look like below:
   *
   * <pre><code class="language-java">
   * private final Scorer scorer; // set via constructor
   *
   * public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
   *   collector.setScorer(scorer);
   *   int doc = scorer.docID();
   *   if (doc &lt; min) {
   *     doc = scorer.advance(min);
   *   }
   *   while (doc &lt; max) {
   *     if (acceptDocs == null || acceptDocs.get(doc)) {
   *       collector.collect(doc);
   *     }
   *     doc = scorer.nextDoc();
   *   }
   *   return doc;
   * }
   * </code></pre>
   *
   * @param collector The collector to which all matching documents are passed.
   * @param acceptDocs {@link Bits} that represents the allowed documents to match, or {@code null}
   *     if they are all allowed to match.
   * @param min Score starting at, including, this document
   * @param max Score up to, but not including, this doc
   * @return an under-estimation of the next matching doc after max
   */
  public abstract int score(LeafCollector collector, Bits acceptDocs, int min, int max)
      throws IOException;

  /** Same as {@link DocIdSetIterator#cost()} for bulk scorers. */
  public abstract long cost();

  /**
   * Materializes all matching document IDs in {@code [0, maxDoc)} into a {@link DocIdSet} together
   * with an exact match count, for use by {@link LRUQueryCache} (see {@link
   * LRUQueryCache#cacheImpl}).
   *
   * <p>Implementations score from document 0 up to {@link DocIdSetIterator#NO_MORE_DOCS} with no
   * {@code acceptDocs} filter.
   *
   * <p>The default representation is chosen from {@link #cost()} versus {@code maxDoc}: when {@code
   * cost * 100 >= maxDoc} (estimated density at least 1%), a dense {@link BitDocIdSet} is used;
   * otherwise a sparse {@link RoaringDocIdSet} is built.
   *
   * @param maxDoc one past the maximum document ID in the index (e.g. {@link
   *     org.apache.lucene.index.LeafReader#maxDoc()})
   * @return the cached doc-id set and its cardinality
   * @throws IOException if scoring fails
   * @see LRUQueryCache.CacheAndCount
   */
  public LRUQueryCache.CacheAndCount intoCacheAndCount(int maxDoc) throws IOException {
    if (cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      return cacheIntoBitSet(maxDoc);
    } else {
      return cacheIntoRoaringDocIdSet(maxDoc);
    }
  }

  private LRUQueryCache.CacheAndCount cacheIntoBitSet(int maxDoc) throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    int[] count = new int[1];
    score(
        new LeafCollector() {

          private int[] buffer;

          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            count[0]++;
            bitSet.set(doc);
          }

          @Override
          public void collectRange(int min, int max) {
            count[0] += max - min;
            bitSet.set(min, max);
          }

          @Override
          public void collect(DocIdStream stream) {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              for (int i = 0; i < c; ++i) {
                bitSet.set(buffer[i]);
              }
              count[0] += c;
            }
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    return new LRUQueryCache.CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
  }

  private LRUQueryCache.CacheAndCount cacheIntoRoaringDocIdSet(int maxDoc) throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    score(
        new LeafCollector() {

          private int[] buffer = null;

          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            builder.add(doc);
          }

          @Override
          public void collectRange(int min, int max) {
            builder.add(min, max);
          }

          @Override
          public void collect(DocIdStream stream) {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              for (int i = 0; i < c; ++i) {
                builder.add(buffer[i]);
              }
            }
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    RoaringDocIdSet cache = builder.build();
    return new LRUQueryCache.CacheAndCount(cache, cache.cardinality());
  }
}
