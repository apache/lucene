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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsReader.Candidates;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TimeLimitingKnnCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link KnnFloatVectorQuery} for SegmentIVF fields.
 *
 * <p>Every segment contributes its deduplicated coarse shortlist; these are merged into one
 * index-wide shortlist of {@value SegmentIVFVectorsReader#RERANK_PER_K} candidates per requested
 * neighbor (at least {@value SegmentIVFVectorsReader#MIN_RERANK}), and only those fine records are
 * read and scored, so fine reads stay constant as the segment count grows. A dense filter reaches
 * the codec as a per-segment bit set; a filter that rewrites to a single clause falls back to a
 * plain {@link KnnFloatVectorQuery}, and segments with at most {@code k} matches are searched
 * exactly.
 *
 * @lucene.experimental
 */
public final class SegmentIVFKnnQuery extends KnnFloatVectorQuery {
  private final Query denseFilter;
  private final Weight filterWeight;
  private final Map<Integer, TopDocs> reranked;

  /** Creates a query probing {@code numProbes} cells, with an optional (possibly null) filter. */
  public SegmentIVFKnnQuery(String field, float[] target, int k, Query filter, int numProbes) {
    this(field, target, k, filter, new SearchStrategy(numProbes));
  }

  /** Creates a query with an explicit probe strategy and an optional (possibly null) filter. */
  public SegmentIVFKnnQuery(
      String field, float[] target, int k, Query filter, SearchStrategy strategy) {
    super(field, target, k, null, Objects.requireNonNull(strategy));
    this.denseFilter = filter;
    this.filterWeight = null;
    this.reranked = null;
  }

  private SegmentIVFKnnQuery(
      SegmentIVFKnnQuery query, Weight filterWeight, Map<Integer, TopDocs> reranked) {
    super(query.field, query.target, query.k, null, query.searchStrategy);
    this.denseFilter = query.denseFilter;
    this.filterWeight = filterWeight;
    this.reranked = reranked;
  }

  /** Intersects the filter with the field and pre-creates its weight for segment search. */
  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    if (reranked != null || filterWeight != null) return super.rewrite(searcher);
    Weight filter = null;
    if (denseFilter != null) {
      var both = new BooleanQuery.Builder();
      both.add(denseFilter, Occur.FILTER).add(new FieldExistsQuery(field), Occur.FILTER);
      Query rewritten = searcher.rewrite(both.build());
      if (rewritten.getClass() == MatchNoDocsQuery.class) return rewritten;
      filter = rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }
    Map<Integer, TopDocs> global = globalRerank(searcher, filter);
    if (global != null) return new SegmentIVFKnnQuery(this, null, global).rewrite(searcher);
    // Some segment is not SegmentIVF: search every segment on its own.
    return filter == null
        ? super.rewrite(searcher)
        : new SegmentIVFKnnQuery(this, filter, null).rewrite(searcher);
  }

  /** Returns a segment's share of the finished global rerank, when there is one. */
  @Override
  protected TopDocs searchLeaf(
      LeafReaderContext context, Weight filter, TimeLimitingKnnCollectorManager manager)
      throws IOException {
    if (reranked == null) return super.searchLeaf(context, filter, manager);
    return reranked.getOrDefault(context.ord, TopDocsCollector.EMPTY_TOPDOCS);
  }

  /**
   * Merges every segment's coarse shortlist into one index-wide shortlist and fine-reranks only
   * that, returning each segment's top hits with global doc IDs, or null when some segment is not
   * searched by SegmentIVF.
   */
  private Map<Integer, TopDocs> globalRerank(IndexSearcher searcher, Weight filter)
      throws IOException {
    SearchStrategy strategy = (SearchStrategy) searchStrategy;
    List<LeafReaderContext> leaves = new ArrayList<>();
    List<SegmentIVFVectorsReader> readers = new ArrayList<>();
    List<Callable<Candidates>> scans = new ArrayList<>();
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      if (context.reader().getFieldInfos().fieldInfo(field) == null) continue;
      if (FilterLeafReader.unwrap(context.reader()) instanceof CodecReader codec
          && codec.getVectorReader() instanceof KnnVectorsReader vectors
          && vectors.unwrapReaderForField(field) instanceof SegmentIVFVectorsReader sivf) {
        leaves.add(context);
        readers.add(sivf);
        scans.add(
            () -> {
              AcceptDocs accept = accepted(context, filter);
              return accept == null
                  ? Candidates.EMPTY
                  : sivf.candidates(field, target, k, strategy, accept);
            });
      } else {
        return null; // Some segment is not SegmentIVF.
      }
    }
    List<Candidates> candidates = searcher.getTaskExecutor().invokeAll(scans);

    // Keep the globally nearest candidates by coarse distance: (distance, segment, position).
    int total = 0;
    for (Candidates c : candidates) total += c.slots().length;
    long[] ranked = new long[total];
    for (int l = 0, at = 0; l < candidates.size(); l++) {
      int[] distances = candidates.get(l).distances();
      for (int i = 0; i < distances.length; i++) {
        ranked[at++] = (long) distances[i] << 40 | (long) l << 20 | i;
      }
    }
    Arrays.sort(ranked);
    int keep = (int) Math.min(total, SegmentIVFVectorsReader.rerankCount(k));
    int[] counts = new int[leaves.size()];
    for (int i = 0; i < keep; i++) counts[(int) (ranked[i] >>> 20) & 0xFFFFF]++;
    int[][] slots = new int[leaves.size()][];
    for (int l = 0; l < slots.length; l++) slots[l] = new int[counts[l]];
    Arrays.fill(counts, 0);
    for (int i = 0; i < keep; i++) {
      int l = (int) (ranked[i] >>> 20) & 0xFFFFF;
      slots[l][counts[l]++] = candidates.get(l).slots()[(int) ranked[i] & 0xFFFFF];
    }

    List<Callable<TopDocs>> reranks = new ArrayList<>();
    List<Integer> ords = new ArrayList<>();
    for (int l = 0; l < leaves.size(); l++) {
      if (slots[l].length == 0) continue;
      LeafReaderContext context = leaves.get(l);
      SegmentIVFVectorsReader reader = readers.get(l);
      Candidates from = candidates.get(l);
      int[] leafSlots = slots[l];
      ords.add(context.ord);
      reranks.add(
          () -> {
            TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
            reader.rerank(from, leafSlots, collector);
            TopDocs hits = collector.topDocs();
            for (ScoreDoc hit : hits.scoreDocs) hit.doc += context.docBase;
            return hits;
          });
    }
    List<TopDocs> hits = searcher.getTaskExecutor().invokeAll(reranks);
    Map<Integer, TopDocs> byLeaf = new HashMap<>();
    for (int i = 0; i < hits.size(); i++) byLeaf.put(ords.get(i), hits.get(i));
    return byLeaf;
  }

  /** Searches one segment on its own, with the filter materialized into a bit set. */
  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context, AcceptDocs live, int limit, KnnCollectorManager manager)
      throws IOException {
    if (filterWeight == null) return super.approximateSearch(context, live, limit, manager);
    AcceptDocs accept = accepted(context, filterWeight);
    if (accept == null) return TopDocsCollector.EMPTY_TOPDOCS;
    return super.approximateSearch(context, accept, accept.cost() + 1, manager);
  }

  /**
   * Returns the segment's live documents, intersected with the filter as a bit set when there is
   * one, or null when nothing in the segment is accepted.
   */
  private static AcceptDocs accepted(LeafReaderContext context, Weight filter) throws IOException {
    int maxDoc = context.reader().maxDoc();
    if (filter == null) return AcceptDocs.fromLiveDocs(context.reader().getLiveDocs(), maxDoc);
    BulkScorer scorer = filter.bulkScorer(context);
    if (scorer == null) return null;
    FixedBitSet accepted = new FixedBitSet(maxDoc);
    LeafCollector collector =
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            accepted.set(doc);
          }
        };
    scorer.score(collector, context.reader().getLiveDocs(), 0, maxDoc);
    int n = accepted.cardinality();
    if (n == 0) return null;
    return AcceptDocs.fromIteratorSupplier(() -> new BitSetIterator(accepted, n), null, maxDoc);
  }

  /** Formats the query and its dense filter. */
  @Override
  public String toString(String field) {
    return "SegmentIVFKnnQuery(" + super.toString(field) + ", filter=" + denseFilter + ")";
  }

  /** Compares the base query and dense filter. */
  @Override
  public boolean equals(Object other) {
    return super.equals(other)
        && Objects.equals(denseFilter, ((SegmentIVFKnnQuery) other).denseFilter);
  }

  /** Hashes the base query and dense filter. */
  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hashCode(denseFilter);
  }
}
