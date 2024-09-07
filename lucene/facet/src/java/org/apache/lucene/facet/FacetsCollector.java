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
package org.apache.lucene.facet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * Collects hits for subsequent faceting. Once you've run a search and collect hits into this,
 * instantiate one of the {@link Facets} subclasses to do the facet counting. Use the {@code search}
 * utility methods to perform an "ordinary" search but also collect into a {@link Collector}.
 */
public class FacetsCollector extends SimpleCollector {

  private LeafReaderContext context;
  private Scorable scorer;
  private int totalHits;
  private float[] scores;
  private final boolean keepScores;
  private final List<MatchingDocs> matchingDocs = new ArrayList<>();
  private DocIdSetBuilder docsBuilder;

  /**
   * Holds the documents that were matched in the {@link org.apache.lucene.index.LeafReaderContext}.
   * If scores were required, then {@code scores} is not null.
   */
  public static final class MatchingDocs {

    /** Context for this segment. */
    public final LeafReaderContext context;

    /** Which documents were seen. */
    public final DocIdSet bits;

    /** Non-sparse scores array. */
    public final float[] scores;

    /** Total number of hits */
    public final int totalHits;

    /** Sole constructor. */
    public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits, float[] scores) {
      this.context = context;
      this.bits = bits;
      this.scores = scores;
      this.totalHits = totalHits;
    }
  }

  /** Default constructor */
  public FacetsCollector() {
    this(false);
  }

  /**
   * Create this; if {@code keepScores} is true then a float[] is allocated to hold score of all
   * hits.
   */
  public FacetsCollector(boolean keepScores) {
    this.keepScores = keepScores;
  }

  /** True if scores were saved. */
  public final boolean getKeepScores() {
    return keepScores;
  }

  /** Returns the documents matched by the query, one {@link MatchingDocs} per visited segment. */
  public List<MatchingDocs> getMatchingDocs() {
    return matchingDocs;
  }

  @Override
  public void collect(int doc) throws IOException {
    docsBuilder.grow(1).add(doc);
    if (keepScores) {
      if (totalHits >= scores.length) {
        float[] newScores = new float[ArrayUtil.oversize(totalHits + 1, 4)];
        System.arraycopy(scores, 0, newScores, 0, totalHits);
        scores = newScores;
      }
      scores[totalHits] = scorer.score();
    }
    totalHits++;
  }

  @Override
  public ScoreMode scoreMode() {
    return keepScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public final void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    assert docsBuilder == null;
    docsBuilder = new DocIdSetBuilder(context.reader().maxDoc());
    totalHits = 0;
    if (keepScores) {
      scores = new float[64]; // some initial size
    }
    this.context = context;
  }

  @Override
  public void finish() throws IOException {
    DocIdSet bits;
    if (docsBuilder != null) {
      bits = docsBuilder.build();
      docsBuilder = null;
    } else {
      bits = DocIdSet.EMPTY;
    }
    matchingDocs.add(new MatchingDocs(this.context, bits, totalHits, scores));
    scores = null;
    context = null;
  }

  /**
   * Reduces matching docs held by the provided facets collectors, merging matching docs for the
   * same leaf into a single matching docs instance
   *
   * @param facetsCollectors the facets collectors
   * @return the reduced matching docs, with one instance per leaf reader context
   */
  protected static Collection<MatchingDocs> reduceMatchingDocs(
      final Collection<? extends FacetsCollector> facetsCollectors) {
    // When a segment is split into partitions, each partition gets its own FacetsCollector that
    // pulls doc_values independently, and builds a bitset of the size of the entire segment. When
    // segments are partitioned, each partition will collect only the docs in its docid range, hence
    // there will be multiple MatchingDocs pointing to the same LeafReaderContext. As part of the
    // reduction we merge back partitions into a single MatchingDocs per segment.
    Map<LeafReaderContext, MatchingDocs> matchingDocsMap = new HashMap<>();
    for (FacetsCollector facetsCollector : facetsCollectors) {
      for (FacetsCollector.MatchingDocs matchingDocs : facetsCollector.getMatchingDocs()) {
        matchingDocsMap.compute(
            matchingDocs.context,
            (leafReaderContext, existing) -> {
              if (existing == null) {
                return matchingDocs;
              }
              return merge(existing, matchingDocs);
            });
      }
    }
    return matchingDocsMap.values();
  }

  private static FacetsCollector.MatchingDocs merge(
      FacetsCollector.MatchingDocs matchingDocs1, FacetsCollector.MatchingDocs matchingDocs2) {
    assert matchingDocs1.context == matchingDocs2.context;
    // TODO need to merge scores as well
    assert matchingDocs1.scores == null && matchingDocs2.scores == null;
    DocIdSetBuilder docIdSetBuilder = new DocIdSetBuilder(matchingDocs1.context.reader().maxDoc());
    try {
      docIdSetBuilder.add(matchingDocs1.bits.iterator());
      docIdSetBuilder.add(matchingDocs2.bits.iterator());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    int totalHits = matchingDocs1.totalHits + matchingDocs2.totalHits;
    return new FacetsCollector.MatchingDocs(
        matchingDocs1.context, docIdSetBuilder.build(), totalHits, null);
  }
}
