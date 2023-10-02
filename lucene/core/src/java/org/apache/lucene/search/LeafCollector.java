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
import org.apache.lucene.index.StoredFields;

/**
 * Collector decouples the score from the collected doc: the score computation is skipped entirely
 * if it's not needed. Collectors that do need the score should implement the {@link #setScorer}
 * method, to hold onto the passed {@link Scorer} instance, and call {@link Scorer#score()} within
 * the collect method to compute the current hit's score. If your collector may request the score
 * for a single hit multiple times, you should use {@link ScoreCachingWrappingScorer}.
 *
 * <p><b>NOTE:</b> The doc that is passed to the collect method is relative to the current reader.
 * If your collector needs to resolve this to the docID space of the Multi*Reader, you must re-base
 * it by recording the docBase from the most recent setNextReader call. Here's a simple example
 * showing how to collect docIDs into a BitSet:
 *
 * <pre class="prettyprint">
 * IndexSearcher searcher = new IndexSearcher(indexReader);
 * final BitSet bits = new BitSet(indexReader.maxDoc());
 * searcher.search(query, new Collector() {
 *
 *   public LeafCollector getLeafCollector(LeafReaderContext context)
 *       throws IOException {
 *     final int docBase = context.docBase;
 *     return new LeafCollector() {
 *
 *       <em>// ignore scorer</em>
 *       public void setScorer(Scorer scorer) throws IOException {
 *       }
 *
 *       public void collect(int doc) throws IOException {
 *         bits.set(docBase + doc);
 *       }
 *
 *     };
 *   }
 *
 * });
 * </pre>
 *
 * <p>Not all collectors will need to rebase the docID. For example, a collector that simply counts
 * the total number of hits would skip it.
 *
 * @lucene.experimental
 */
public interface LeafCollector {

  /**
   * Called before successive calls to {@link #collect(int)}. Implementations that need the score of
   * the current document (passed-in to {@link #collect(int)}), should save the passed-in Scorer and
   * call scorer.score() when needed.
   */
  void setScorer(Scorable scorer) throws IOException;

  /**
   * Called once for every document matching a query, with the unbased document number.
   *
   * <p>Note: The collection of the current segment can be terminated by throwing a {@link
   * CollectionTerminatedException}. In this case, the last docs of the current {@link
   * org.apache.lucene.index.LeafReaderContext} will be skipped and {@link IndexSearcher} will
   * swallow the exception and continue collection with the next leaf.
   *
   * <p>Note: This is called in an inner search loop. For good search performance, implementations
   * of this method should not call {@link StoredFields#document} on every hit. Doing so can slow
   * searches by an order of magnitude or more.
   */
  void collect(int doc) throws IOException;

  /**
   * Bulk-collect doc IDs.
   *
   * <p>Note: The provided {@link DocIdStream} may be reused across calls and should be consumed
   * immediately.
   *
   * <p>Note: The provided {@link DocIdStream} typically only holds a small subset of query matches.
   * This method may be called multiple times per segment.
   *
   * <p>Like {@link #collect(int)}, it is guaranteed that doc IDs get collected in order, ie. doc
   * IDs are collected in order within a {@link DocIdStream}, and if called twice, all doc IDs from
   * the second {@link DocIdStream} will be greater than all doc IDs from the first {@link
   * DocIdStream}.
   *
   * <p>It is legal for callers to mix calls to {@link #collect(DocIdStream)} and {@link
   * #collect(int)}.
   *
   * <p>The default implementation calls {@code stream.forEach(this::collect)}.
   */
  default void collect(DocIdStream stream) throws IOException {
    stream.forEach(this::collect);
  }

  /**
   * Optionally returns an iterator over competitive documents.
   *
   * <p>Collectors should delegate this method to their comparators if their comparators provide the
   * skipping functionality over non-competitive docs.
   *
   * <p>The default is to return {@code null} which is interpreted as the collector provide any
   * competitive iterator.
   */
  default DocIdSetIterator competitiveIterator() throws IOException {
    return null;
  }

  /**
   * Hook that gets called once the leaf that is associated with this collector has finished
   * collecting successfully, including when a {@link CollectionTerminatedException} is thrown. This
   * is typically useful to compile data that has been collected on this leaf, e.g. to convert facet
   * counts on leaf ordinals to facet counts on global ordinals. The default implementation does
   * nothing.
   */
  default void finish() throws IOException {}
}
