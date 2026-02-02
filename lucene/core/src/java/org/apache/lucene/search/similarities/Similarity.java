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
package org.apache.lucene.search.similarities;

import java.util.Collections;
import java.util.Objects;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.SmallFloat;

/**
 * Similarity defines the components of Lucene scoring.
 *
 * <p>Expert: Scoring API.
 *
 * <p>This is a low-level API, you should only extend this API if you want to implement an
 * information retrieval <i>model</i>. If you are instead looking for a convenient way to alter
 * Lucene's scoring, consider just tweaking the default implementation: {@link BM25Similarity} or
 * extend {@link SimilarityBase}, which makes it easy to compute a score from index statistics.
 *
 * <p>Similarity determines how Lucene weights terms, and Lucene interacts with this class at both
 * <a href="#indextime">index-time</a> and <a href="#querytime">query-time</a>.
 *
 * <p><a id="indextime">Indexing Time</a> At indexing time, the indexer calls {@link
 * #computeNorm(FieldInvertState)}, allowing the Similarity implementation to set a per-document
 * value for the field that will be later accessible via {@link
 * org.apache.lucene.index.LeafReader#getNormValues(String)}. Lucene makes no assumption about what
 * is in this norm, but it is most useful for encoding length normalization information.
 *
 * <p>Implementations should carefully consider how the normalization is encoded: while Lucene's
 * default implementation encodes length normalization information with {@link SmallFloat} into a
 * single byte, this might not be suitable for all purposes.
 *
 * <p>Many formulas require the use of average document length, which can be computed via a
 * combination of {@link CollectionStatistics#sumTotalTermFreq()} and {@link
 * CollectionStatistics#docCount()}.
 *
 * <p>Additional scoring factors can be stored in named {@link NumericDocValuesField}s and accessed
 * at query-time with {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)}.
 * However this should not be done in the {@link Similarity} but externally, for instance by using
 * <code>FunctionScoreQuery</code>.
 *
 * <p>Finally, using index-time boosts (either via folding into the normalization byte or via
 * DocValues), is an inefficient way to boost the scores of different fields if the boost will be
 * the same for every document, instead the Similarity can simply take a constant boost parameter
 * <i>C</i>, and {@link PerFieldSimilarityWrapper} can return different instances with different
 * boosts depending upon field name.
 *
 * <p><a id="querytime">Query time</a> At query-time, Queries interact with the Similarity via these
 * steps:
 *
 * <ol>
 *   <li>The {@link #scorer(float, CollectionStatistics, TermStatistics...)} method is called a
 *       single time, allowing the implementation to compute any statistics (such as IDF, average
 *       document length, etc) across <i>the entire collection</i>. The {@link TermStatistics} and
 *       {@link CollectionStatistics} passed in already contain all of the raw statistics involved,
 *       so a Similarity can freely use any combination of statistics without causing any additional
 *       I/O. Lucene makes no assumption about what is stored in the returned {@link
 *       Similarity.SimScorer} object.
 *   <li>Then {@link SimScorer#score(float, long)} is called for every matching document to compute
 *       its score.
 * </ol>
 *
 * <p><a id="explaintime">Explanations</a> When {@link
 * IndexSearcher#explain(org.apache.lucene.search.Query, int)} is called, queries consult the
 * Similarity's DocScorer for an explanation of how it computed its score. The query passes in a the
 * document id and an explanation of how the frequency was computed.
 *
 * @see org.apache.lucene.index.IndexWriterConfig#setSimilarity(Similarity)
 * @see IndexSearcher#setSimilarity(Similarity)
 * @lucene.experimental
 */
public abstract class Similarity {
  /**
   * True if overlap tokens (tokens with a position of increment of zero) are discounted from the
   * document's length.
   */
  private final boolean discountOverlaps;

  /**
   * Returns true if overlap tokens are discounted from the document's length.
   *
   * @see #computeNorm
   */
  public final boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  /** Default constructor. (For invocation by subclass constructors, typically implicit.) */
  protected Similarity() {
    this(true);
  }

  /**
   * Expert constructor that allows adjustment of {@link #getDiscountOverlaps()} at index-time.
   *
   * <p>Overlap tokens are tokens such as synonyms, that have a {@link PositionIncrementAttribute}
   * of zero from the analysis chain.
   *
   * <p><b>NOTE</b>: If you modify this parameter, you'll need to re-index for it to take effect.
   *
   * @param discountOverlaps true if overlap tokens should not impact document length for scoring.
   */
  protected Similarity(boolean discountOverlaps) {
    this.discountOverlaps = discountOverlaps;
  }

  /**
   * Computes the normalization value for a field at index-time.
   *
   * <p>The default implementation uses {@link SmallFloat#intToByte4} to encode the number of terms
   * as a single byte.
   *
   * <p><b>WARNING</b>: The default implementation is used by Lucene's supplied Similarity classes,
   * which means you can change the Similarity at runtime without reindexing. If you override this
   * method, you'll need to re-index documents for it to take effect.
   *
   * <p>Matches in longer fields are less precise, so implementations of this method usually set
   * smaller values when <code>state.getLength()</code> is large, and larger values when <code>
   * state.getLength()</code> is small.
   *
   * <p>Note that for a given term-document frequency, greater unsigned norms must produce scores
   * that are lower or equal, ie. for two encoded norms {@code n1} and {@code n2} so that {@code
   * Long.compareUnsigned(n1, n2) > 0} then {@code SimScorer.score(freq, n1) <=
   * SimScorer.score(freq, n2)} for any legal {@code freq}.
   *
   * <p>{@code 0} is not a legal norm, so {@code 1} is the norm that produces the highest scores.
   *
   * @lucene.experimental
   * @param state accumulated state of term processing for this field
   * @return computed norm value
   */
  public long computeNorm(FieldInvertState state) {
    final int numTerms;
    if (state.getIndexOptions() == IndexOptions.DOCS) {
      numTerms = state.getUniqueTermCount();
    } else if (discountOverlaps) {
      numTerms = state.getLength() - state.getNumOverlap();
    } else {
      numTerms = state.getLength();
    }
    return SmallFloat.intToByte4(numTerms);
  }

  /**
   * Compute any collection-level weight (e.g. IDF, average document length, etc) needed for scoring
   * a query.
   *
   * @param boost a multiplicative factor to apply to the produces scores
   * @param collectionStats collection-level statistics, such as the number of tokens in the
   *     collection.
   * @param termStats term-level statistics, such as the document frequency of a term across the
   *     collection.
   * @return SimWeight object with the information this Similarity needs to score a query.
   */
  public abstract SimScorer scorer(
      float boost, CollectionStatistics collectionStats, TermStatistics... termStats);

  /**
   * Stores the weight for a query across the indexed collection. This abstract implementation is
   * empty; descendants of {@code Similarity} should subclass {@code SimWeight} and define the
   * statistics they require in the subclass. Examples include idf, average field length, etc.
   */
  public abstract static class SimScorer {

    /** Sole constructor. (For invocation by subclass constructors.) */
    protected SimScorer() {}

    /**
     * Score a single document. {@code freq} is the document-term sloppy frequency and must be
     * finite and positive. {@code norm} is the encoded normalization factor as computed by {@link
     * Similarity#computeNorm(FieldInvertState)} at index time, or {@code 1} if norms are disabled.
     * {@code norm} is never {@code 0}.
     *
     * <p>Score must not decrease when {@code freq} increases, ie. if {@code freq1 > freq2}, then
     * {@code score(freq1, norm) >= score(freq2, norm)} for any value of {@code norm} that may be
     * produced by {@link Similarity#computeNorm(FieldInvertState)}.
     *
     * <p>Score must not increase when the unsigned {@code norm} increases, ie. if {@code
     * Long.compareUnsigned(norm1, norm2) > 0} then {@code score(freq, norm1) <= score(freq, norm2)}
     * for any legal {@code freq}.
     *
     * <p>As a consequence, the maximum score that this scorer can produce is bound by {@code
     * score(Float.MAX_VALUE, 1)}.
     *
     * @param freq sloppy term frequency, must be finite and positive
     * @param norm encoded normalization factor or {@code 1} if norms are disabled
     * @return document's score
     */
    public abstract float score(float freq, long norm);

    /**
     * Return a {@link BulkSimScorer} that produces the exact same scores as this {@link SimScorer}
     * but is more efficient at bulk-computing scores.
     *
     * <p><b>NOTE</b>: The returned instance is not thread-safe.
     */
    public BulkSimScorer asBulkSimScorer() {
      return new DefaultBulkSimScorer(this);
    }

    /**
     * Explain the score for a single document
     *
     * @param freq Explanation of how the sloppy term frequency was computed
     * @param norm encoded normalization factor, as returned by {@link Similarity#computeNorm}, or
     *     {@code 1} if norms are disabled
     * @return document's score
     */
    public Explanation explain(Explanation freq, long norm) {
      return Explanation.match(
          score(freq.getValue().floatValue(), norm),
          "score(freq=" + freq.getValue() + "), with freq of:",
          Collections.singleton(freq));
    }
  }

  /** Specialization of {@link SimScorer} for bulk-computation of scores. */
  public interface BulkSimScorer {

    /**
     * Bulk computation of scores. For each index {@code i} in [0, size), scores[i] is computed as
     * score(freqs[i], norms[i]). The default implementation does the following:
     *
     * <pre><code class="language-java">
     * for (int i = 0; i &lt; size; ++i) {
     *   scores[i] = score(freqs[i], norms[i]);
     * }
     * </code></pre>
     *
     * <p><b>NOTE</b>: It is legal to pass the same {@code freqs} and {@code scores} arrays.
     */
    void score(int size, float[] freqs, long[] norms, float[] scores);
  }

  private static class DefaultBulkSimScorer implements BulkSimScorer {

    private final SimScorer scorer;

    DefaultBulkSimScorer(SimScorer scorer) {
      this.scorer = Objects.requireNonNull(scorer);
    }

    @Override
    public void score(int size, float[] freqs, long[] norms, float[] scores) {
      for (int i = 0; i < size; ++i) {
        scores[i] = scorer.score(freqs[i], norms[i]);
      }
    }
  }
}
