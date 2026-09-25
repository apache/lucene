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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * The ivfaster cells of a query as a {@link Scorer}, so a kNN clause is evaluated doc-at-a-time
 * alongside every other clause of a {@code BooleanQuery}.
 *
 * <p>{@code KnnFloatVectorQuery} runs its search during rewrite and becomes a fixed set of doc ids,
 * so it can only receive a filter the query builder resolved for it up front. This query instead
 * exposes the codec's structure directly: per segment it selects the {@code nprobe} nearest cells
 * the way a search does, Hamming-scores their runs in bulk, and its scorer's iterator is the set of
 * documents the coarse cut ADMITS, in doc order. Inside a conjunction the sparser clause leads, and
 * that set is advanced to whatever it yields.
 *
 * <p>ONE ITERATOR, NOT A DISJUNCTION OVER THE CELLS. The cells are already doc-sorted posting lists
 * ({@link CellPostings}) and unioning them is the obvious construction, but a rarity-widened probe
 * makes it dozens of sub-iterators, and Lucene's disjunction then rescans whatever does not fit in
 * its heap on every advance — 62% of query samples at one filter clause, 74% at four. Since the
 * admission cut is FIXED before the walk (below), the union can be built once, in the same pass
 * that applies the cut, and walked as a bit set; see {@code CellSession#admittedDocs}.
 *
 * <p>Admission is therefore not a scoring phase: the cut is the coarse distance that admits {@code
 * admit} rows, the same histogram cut the codec's own scan takes, and it is applied when the scorer
 * is built rather than per document. There is no {@link TwoPhaseIterator}, because every document
 * the iterator yields already matches. {@code score()} is the fine tier — one record read and one
 * int8 dot — computed only for documents the whole conjunction yields.
 *
 * <p>THE PROBE IS SIZED UP FRONT FROM THE LEADING CLAUSE. A scorer emits documents in ascending
 * order, so cells cannot be added once the walk has passed their first documents, and the codec's
 * own filtered search, which widens as it goes, has no equivalent here. What a scorer does get is
 * {@code leadCost}, the cardinality of the clause that will lead the conjunction: when a filter is
 * sparser than this clause, the probe is sized so that the expected accepted documents in the
 * probed cells reach the codec's filtered target, and a filter narrow enough that reranking every
 * accepted document is cheaper than any walk gets exactly that. See {@code CellScorer}.
 *
 * <p>Scores are the field's similarity on the collector's scale, the same values {@code
 * KnnFloatVectorQuery} produces from this codec. There is no max-score bound, so the query does not
 * participate in dynamic pruning.
 *
 * @lucene.experimental
 */
public final class IVFasterKnnQuery extends Query {

  private final String field;
  private final float[] target;
  private final int nprobe;
  private final int admit;
  private final boolean adaptive;

  /** Sizing only; never consulted for matching. See the six-argument constructor. */
  private final Query filterForSizing;

  /** Optional pairwise dependence between filter clauses; see {@link ClauseDependence}. */
  private final ClauseDependence dependence;

  /**
   * Pairwise dependence between two filter clauses, consulted in the order a conjunction resolves
   * them. {@code null} means INDEPENDENT, which is the default for every pair.
   *
   * <p>WHY PAIRWISE, AND WHY A SLOT PER LEVEL. The exact selectivity of a conjunction is the chain
   * rule over its resolution order, {@code P(A1) * P(A2|A1) * P(A3|A1^A2) * ...}. That order is
   * known before the walk — {@code ConjunctionDISI} tim-sorts clauses by {@code cost()} so the
   * sparsest leads — which is what keeps this a chain of {@code m} terms rather than a lattice of
   * {@code 2^m} conditionals. Truncating each conditional to its pairwise interactions with the
   * ALREADY-RESOLVED prefix turns the chain into a dynamic program over {@code m*(m-1)/2} slots: at
   * level {@code i}, one slot per earlier clause {@code j}.
   *
   * <p>Returning {@code null} costs nothing and contributes a factor of exactly 1, so a caller only
   * populates the pairs it actually knows about and everything else stays independent. Populating
   * nothing reproduces the independence product exactly.
   *
   * <p>WHAT THIS CANNOT CAPTURE. Three-way and higher structure. Two predicates can be pairwise
   * independent and jointly determined ({@code C = A xor B}), and no pairwise slot sees that. It is
   * a second-order approximation, deliberately.
   *
   * <p>When this is supplied it is used INSTEAD of sampling the chain, on the assumption that a
   * caller with schema knowledge knows better than a bounded sample. Supply nothing to have the
   * chain measured instead; see {@code conjunctionCost}.
   *
   * @lucene.experimental
   */
  public interface ClauseDependence {
    /**
     * The lift {@code P(a|b) / P(a)}, or {@code null} if {@code a} and {@code b} are independent.
     *
     * <p>Greater than 1 means the two co-occur more than chance (the conjunction is LESS selective
     * than independence predicts); below 1 means they exclude each other. {@code 0} would assert
     * the two never co-occur, and is clamped away from an empty conjunction by the caller.
     *
     * @param a the clause being conditioned, at level {@code i} of the resolution order
     * @param b an earlier clause in that order
     */
    Double lift(Query a, Query b);
  }

  /**
   * @param field the ivfaster vector field
   * @param target the query vector, copied
   * @param nprobe cells to probe per segment, or {@code 0} for the value the segment was written
   *     with
   * @param admit coarse admission width per segment, or {@code 0} for the codec's shortlist size
   * @param adaptive whether the probe and the cut are sized from the cost of the clause that leads
   *     the conjunction this query sits in; see the class javadoc. An adaptive query's matches
   *     depend on that clause, which the query-level consistency checks in Lucene's test framework
   *     do not expect, so a query meant to be checked by them must be non-adaptive.
   */
  public IVFasterKnnQuery(String field, float[] target, int nprobe, int admit, boolean adaptive) {
    this(field, target, nprobe, admit, adaptive, null);
  }

  /**
   * As above, plus the conjunction this clause will sit beside, FOR SIZING ONLY.
   *
   * <p>WHY THE FILTER HAS TO BE HANDED OVER. Probe width is chosen from filter rarity, and without
   * this argument the only evidence available is {@code leadCost} — which a conjunction reports as
   * the MINIMUM of its clause costs, never their product. {@code min} is an upper bound on the size
   * of the intersection, so it always UNDER-states rarity and therefore always under-probes;
   * measured, a four-clause filter of true selectivity 5% cost 24 recall points that way. Lucene's
   * own {@code KnnFloatVectorQuery} takes its filter for the same reason: a clause cannot see its
   * siblings, so a query that needs to plan against a filter must be given it.
   *
   * <p>SIZING ONLY — this does not change what matches. Pass the same {@code Query} that is also
   * the {@code FILTER} clause of the enclosing {@code BooleanQuery}; the conjunction still does the
   * matching and the leapfrog is untouched. Passing something else only mis-sizes the probe, it
   * cannot produce a wrong document.
   *
   * @param filterForSizing the conjunction beside this clause, or {@code null} to size from {@code
   *     leadCost} alone
   */
  public IVFasterKnnQuery(
      String field,
      float[] target,
      int nprobe,
      int admit,
      boolean adaptive,
      Query filterForSizing) {
    this.field = Objects.requireNonNull(field, "field");
    this.target = Objects.requireNonNull(target, "target").clone();
    if (nprobe < 0) {
      throw new IllegalArgumentException("nprobe must be >= 0, got " + nprobe);
    }
    if (admit < 0) {
      throw new IllegalArgumentException("admit must be >= 0, got " + admit);
    }
    this.nprobe = nprobe;
    this.admit = admit == 0 ? IVFasterVectorsReader.bruteN() : admit;
    this.adaptive = adaptive;
    this.filterForSizing = filterForSizing;
    this.dependence = null;
  }

  private IVFasterKnnQuery(IVFasterKnnQuery from, ClauseDependence dependence) {
    this.field = from.field;
    this.target = from.target;
    this.nprobe = from.nprobe;
    this.admit = from.admit;
    this.adaptive = from.adaptive;
    this.filterForSizing = from.filterForSizing;
    this.dependence = dependence;
  }

  /**
   * A copy that resolves conjunction selectivity through {@code dependence} instead of sampling the
   * chain; see {@link ClauseDependence}. Only meaningful alongside a {@code filterForSizing}.
   */
  public IVFasterKnnQuery withClauseDependence(ClauseDependence dependence) {
    return new IVFasterKnnQuery(this, dependence);
  }

  /** As above, adaptive, with the segment's persisted {@code nprobe} and the codec's width. */
  public IVFasterKnnQuery(String field, float[] target) {
    this(field, target, 0, 0, true);
  }

  /**
   * Correction applied to the independence estimate of a conjunction's selectivity; {@code 1.0} IS
   * the independence assumption and is the default.
   *
   * <p>Independence is what clause costs can support on their own: {@code cost()} exposes only the
   * MARGINAL cardinality of each clause, and no set of marginals determines the size of an
   * intersection. Ordering the clauses by selectivity — which a conjunction already does — gives
   * the order to combine them in, not the conditionals; {@code P(B|A)} has to be observed, not
   * derived.
   *
   * <p>WHICH WAY IT IS WRONG MATTERS. Positively correlated clauses intersect MORE than
   * independence predicts, so the estimate over-states rarity and over-probes: latency, not recall.
   * Negatively correlated clauses — {@code color=red AND family=blue} — intersect less, so it
   * under-probes and loses recall. Set this below 1.0 for a workload known to chain that way.
   */
  private static final double FILTER_CONJUNCTION_CORRECTOR =
      Double.parseDouble(System.getProperty("ivfaster.filterConjunctionCorrector", "1.0"));

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new Weight(this) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final IVFasterVectorsReader reader = ivfasterReader(context.reader());
        if (reader == null) {
          // No reachable cell structure; see ivfasterReader. Degrade to the leaf's own kNN search,
          // which returns the same top-k a KnnFloatVectorQuery would, so this clause yields CORRECT
          // documents and merely loses the leapfrog. Silently yielding none is not an option: it is
          // indistinguishable from "no document matches" to everything downstream.
          return fallbackSupplier(context, boost);
        }
        final IVFasterVectorsReader.CellSession session =
            reader.openCellSession(field, target, nprobe);
        if (session == null) {
          return null;
        }
        // What the configured probe would walk, before any cell is chosen; the conjunction reads
        // it to decide which clause leads.
        // EVERY DOCUMENT WITH A VECTOR, not what the probe would walk.
        //
        // A conjunction elects its CHEAPEST clause to lead and hands every other clause that
        // clause's cardinality as leadCost. Advertising the probe's walk instead -- which is SMALL
        // exactly when nprobe is small -- made this clause win the election at any selectivity
        // above
        // roughly nprobe * slots / nlist, and a winner is told its OWN cost, so it could never see
        // how selective the filter beside it was. Measured at 1M/nlist=1000, that put filtered
        // recall at 0.688 where the acceptDocs path reached 0.898, and made recall jump 0.451 ->
        // 0.929 on nothing but a probe change, because the election flipped.
        //
        // This clause also SHOULD never lead: per document it pays a coarse Hamming test and, on a
        // match, an int8 rerank, so it wants to be driven by something sparser.
        final long fieldCost = Math.max(1, session.count());
        // Independence estimate of the conjunction's size, when the filter was handed over. -1 when
        // it was not, or when the query is not a pure conjunction.
        final long conjunctionCost = conjunctionCost(searcher, context);
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            // NEVER LESS SELECTIVE THAN leadCost: a conjunction cannot admit more documents than
            // its
            // smallest clause, so min() bounds the estimate from above. That makes this change
            // one-directional -- it can only make the filter look rarer than before, i.e. probe at
            // least as widely -- so it cannot regress recall relative to leadCost alone.
            final long effective =
                conjunctionCost < 0 ? leadCost : Math.min(leadCost, conjunctionCost);
            return CellScorer.create(
                session, fieldCost, adaptive ? effective : Long.MAX_VALUE, admit, boost);
          }

          @Override
          public long cost() {
            return fieldCost;
          }
        };
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final Scorer scorer = scorer(context);
        // Straight off the iterator: this scorer has no verification phase, since admission is
        // decided before the walk begins; see AdmittedScorer.
        if (scorer != null && scorer.iterator().advance(doc) == doc) {
          return Explanation.match(
              scorer.score(), "ivfaster fine score of doc " + doc + " in field " + field);
        }
        return Explanation.noMatch("not in a probed cell, or not admitted by the coarse tier");
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  /**
   * Documents sampled from the leading clause when measuring a conjunction's true selectivity;
   * {@code 0} disables sampling and falls back to the independence product.
   *
   * <p>Bounded and small on purpose: this is paid before the walk, per segment, so it has to be
   * negligible next to a kNN query. 2048 samples over a conjunction of a few clauses is a few
   * thousand {@code advance} calls.
   */
  private static final int FILTER_CHAIN_SAMPLES =
      Integer.getInteger("ivfaster.filterChainSamples", 2048);

  /**
   * Estimated size of {@link #filterForSizing}'s intersection on this leaf, or {@code -1} if it
   * cannot be estimated.
   *
   * <p>A query carrying {@code SHOULD} or {@code MUST_NOT} is not a conjunction and is left to
   * report its own cost, since a disjunction's cost is a sum and multiplying it would be nonsense.
   *
   * <p>MEASURED BY CHAIN, NOT ASSUMED. A conjunction resolves its clauses in a KNOWN order — {@code
   * ConjunctionDISI} tim-sorts them by {@code cost()} so the sparsest leads — and that ordering is
   * what makes the exact answer cheap to reach. By the chain rule
   *
   * <pre>  |A1 ^ ... ^ Am| / N  =  P(A1) * P(A2|A1) * P(A3|A1^A2) * ...</pre>
   *
   * which is EXACT and, because the order is fixed, is {@code m} terms rather than the {@code 2^m}
   * of a general lattice of conditionals. For sizing a probe the chain also TELESCOPES: the only
   * number needed is the final intersection, so measuring the leading clause's pass rate through
   * all the remaining clauses collapses every conditional into one quantity, {@code |intersection|
   * ~= passRate * leadCost}. One pass, no independence assumption.
   *
   * <p>Only the pass rate is sampled, never the whole conjunction — resolving it exactly would cost
   * what materializing the filter costs, which is the {@code acceptDocs} path, so an exact
   * conditional chain here would defeat the reason for walking doc-at-a-time at all.
   *
   * <p>THE SAMPLE IS STRIDED ACROSS THE DOC SPACE rather than taken from the front. Accept sets are
   * routinely correlated with doc id — documents arrive in time order and filters select on recency
   * — so sampling the first {@code n} matches of the leading clause would estimate the wrong
   * region. Stride costs nothing extra: {@code advance} is forward-only either way.
   *
   * <p>Falls back to multiplying the clauses' MARGINAL costs as independent probabilities when
   * sampling is disabled or the leading clause yields nothing to sample, scaled in both cases by
   * {@link #FILTER_CONJUNCTION_CORRECTOR}.
   */
  private long conjunctionCost(IndexSearcher searcher, LeafReaderContext context)
      throws IOException {
    if (filterForSizing == null) {
      return -1;
    }
    final List<Query> clauses = new ArrayList<>();
    if (flattenConjunction(filterForSizing, clauses) == false || clauses.isEmpty()) {
      return -1;
    }
    final int maxDoc = context.reader().maxDoc();
    if (maxDoc <= 0) {
      return -1;
    }
    final List<ScorerSupplier> suppliers = new ArrayList<>(clauses.size());
    for (Query clause : clauses) {
      final Weight w =
          searcher.createWeight(searcher.rewrite(clause), ScoreMode.COMPLETE_NO_SCORES, 1f);
      final ScorerSupplier ss = w.scorerSupplier(context);
      if (ss == null) {
        return 0; // a clause matches nothing on this leaf, so the conjunction does too
      }
      suppliers.add(ss);
    }
    if (dependence != null && suppliers.size() > 1) {
      return analyticChain(clauses, suppliers, maxDoc);
    }
    if (FILTER_CHAIN_SAMPLES > 0 && suppliers.size() > 1) {
      final long sampled = sampleChain(suppliers, maxDoc);
      if (sampled >= 0) {
        return Math.max(1, Math.min(maxDoc, sampled));
      }
    }
    double selectivity = 1.0;
    for (ScorerSupplier ss : suppliers) {
      selectivity *= Math.min(1.0, (double) ss.cost() / maxDoc);
      if (selectivity <= 0) {
        return 0;
      }
    }
    selectivity *= FILTER_CONJUNCTION_CORRECTOR;
    final long est = (long) Math.ceil(selectivity * maxDoc);
    return Math.max(1, Math.min(maxDoc, est));
  }

  /**
   * The conditional chain evaluated ANALYTICALLY from {@link ClauseDependence}, rather than
   * measured.
   *
   * <p>Clauses are visited in the resolution order a conjunction will use — ascending {@code
   * cost()}, the same order {@code ConjunctionDISI} sorts into — so the slot consulted at level
   * {@code i} is always against a clause that resolves BEFORE it. That ordering is the whole reason
   * this is a linear walk with {@code m*(m-1)/2} lookups instead of a search over subsets.
   *
   * <p>Each level contributes its own marginal times the lift of every earlier clause on it; a
   * {@code null} slot contributes 1, so an empty {@link ClauseDependence} is exactly the
   * independence product.
   *
   * <p>Clamped at both ends: never larger than the smallest clause (a conjunction cannot admit more
   * than its sparsest member) and never zero, since a mis-specified lift of 0 should mis-size the
   * probe rather than assert the conjunction is empty.
   */
  private long analyticChain(List<Query> clauses, List<ScorerSupplier> suppliers, int maxDoc)
      throws IOException {
    // cost() is declared to throw, so read every one before sorting rather than inside a
    // comparator.
    final long[] costs = new long[suppliers.size()];
    final Integer[] order = new Integer[suppliers.size()];
    for (int i = 0; i < costs.length; i++) {
      costs[i] = suppliers.get(i).cost();
      order[i] = i;
    }
    Arrays.sort(order, (x, y) -> Long.compare(costs[x], costs[y]));
    double selectivity = 1.0;
    long smallest = Long.MAX_VALUE;
    for (int i = 0; i < order.length; i++) {
      final int ci = order[i];
      final long cost = costs[ci];
      smallest = Math.min(smallest, cost);
      double factor = Math.min(1.0, (double) cost / maxDoc);
      for (int j = 0; j < i; j++) {
        final Double lift = dependence.lift(clauses.get(ci), clauses.get(order[j]));
        if (lift != null) {
          factor *= lift;
        }
      }
      selectivity *= factor;
    }
    selectivity *= FILTER_CONJUNCTION_CORRECTOR;
    final long est = (long) Math.ceil(selectivity * maxDoc);
    return Math.max(1, Math.min(Math.min(maxDoc, smallest), est));
  }

  /**
   * The telescoped conditional chain: how many documents the conjunction admits, estimated as the
   * leading clause's cardinality times its measured pass rate through every other clause. Returns
   * {@code -1} if it could not sample, so the caller falls back.
   *
   * <p>The leading clause is the sparsest, chosen the same way {@code ConjunctionDISI} chooses it,
   * so the pass rate being measured is {@code P(A2 ^ ... ^ Am | A1)} — the product of every
   * conditional in the chain after the first, in one quantity.
   *
   * <p>Iterators are pulled from FRESH suppliers used for nothing else. The conjunction that
   * actually runs the query builds its own from the same weights, so consuming these cannot disturb
   * it.
   *
   * <p>Sample positions stride the doc space uniformly instead of taking a prefix; see {@link
   * #conjunctionCost}. Both the stride walk and the verification advance forward only, which is the
   * one thing {@link DocIdSetIterator} guarantees.
   */
  private static long sampleChain(List<ScorerSupplier> suppliers, int maxDoc) throws IOException {
    ScorerSupplier leadSupplier = suppliers.get(0);
    for (ScorerSupplier ss : suppliers) {
      if (ss.cost() < leadSupplier.cost()) {
        leadSupplier = ss;
      }
    }
    final long leadCost = Math.max(1, leadSupplier.cost());
    final DocIdSetIterator lead = leadSupplier.get(Long.MAX_VALUE).iterator();
    final List<DocIdSetIterator> rest = new ArrayList<>(suppliers.size() - 1);
    for (ScorerSupplier ss : suppliers) {
      if (ss != leadSupplier) {
        rest.add(ss.get(Long.MAX_VALUE).iterator());
      }
    }
    if (rest.isEmpty()) {
      return -1;
    }
    // Stride so the samples span the segment. Anchors are doc ids, not match ordinals, because the
    // lead's matches are not uniformly distributed and it is the doc space that must be covered.
    final int wanted = Math.min(FILTER_CHAIN_SAMPLES, maxDoc);
    final int stride = Math.max(1, maxDoc / wanted);
    int examined = 0;
    int survived = 0;
    int anchor = 0;
    while (examined < wanted && anchor < maxDoc) {
      int doc = lead.docID() >= anchor ? lead.docID() : lead.advance(anchor);
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      examined++;
      boolean all = true;
      for (DocIdSetIterator it : rest) {
        // Forward-only: a clause already past this doc cannot contain it.
        if (it.docID() > doc) {
          all = false;
          break;
        }
        if (it.docID() < doc && it.advance(doc) != doc) {
          all = false;
          break;
        }
        if (it.docID() != doc) {
          all = false;
          break;
        }
      }
      if (all) {
        survived++;
      }
      anchor = Math.max(doc + 1, anchor + stride);
    }
    if (examined == 0) {
      return -1;
    }
    // A clause that fell behind the stride cannot be advanced back, so a run of misses caused by
    // exhausting `rest` looks the same as genuine non-agreement. Guard the degenerate case where
    // nothing agreed at all: report one document rather than zero, since the conjunction's true
    // size
    // is unknown, not empty.
    final double passRate = (double) survived / examined;
    return Math.max(1, (long) Math.ceil(passRate * leadCost * FILTER_CONJUNCTION_CORRECTOR));
  }

  /**
   * Collects the conjunctive clauses of {@code q} into {@code out}, returning false if {@code q} is
   * not a pure conjunction.
   */
  private static boolean flattenConjunction(Query q, List<Query> out) {
    if (q instanceof BooleanQuery bq) {
      if (bq.getMinimumNumberShouldMatch() > 0) {
        return false;
      }
      for (BooleanClause c : bq.clauses()) {
        if (c.occur() != BooleanClause.Occur.FILTER && c.occur() != BooleanClause.Occur.MUST) {
          return false;
        }
      }
      for (BooleanClause c : bq.clauses()) {
        if (flattenConjunction(c.query(), out) == false) {
          return false;
        }
      }
      return true;
    }
    out.add(q);
    return true;
  }

  /**
   * The leaf's own kNN search, exposed as a scorer over the documents it returned.
   *
   * <p>WHY THIS EXISTS. This query reaches into the codec for its cells, and some leaves do not
   * expose one -- a {@code ParallelLeafReader} is not a {@code FilterLeafReader}, so it cannot be
   * unwrapped to a {@code CodecReader}. On those leaves there is nothing to leapfrog, but there is
   * still a right answer, and {@code searchNearestVectors} is a {@code LeafReader} method that
   * wrappers delegate.
   *
   * <p>WHAT IS LOST. The documents are chosen BEFORE the conjunction runs, so a filter beside this
   * clause becomes a post-filter on this leaf and recall degrades the way post-filtering does. That
   * is strictly better than the alternative it replaces, which was matching nothing at all.
   */
  private ScorerSupplier fallbackSupplier(LeafReaderContext context, float boost)
      throws IOException {
    final int k = Math.max(1, admit);
    final TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, null);
    context.reader().searchNearestVectors(field, target, collector, null);
    final TopDocs td = collector.topDocs();
    if (td.scoreDocs.length == 0) {
      return null;
    }
    // Ascending doc order is the iterator contract; searchNearestVectors returns by score.
    final int n = td.scoreDocs.length;
    final int[] docs = new int[n];
    final float[] scores = new float[n];
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    java.util.Arrays.sort(
        order, (x, y) -> Integer.compare(td.scoreDocs[x].doc, td.scoreDocs[y].doc));
    for (int i = 0; i < n; i++) {
      docs[i] = td.scoreDocs[order[i]].doc;
      scores[i] = td.scoreDocs[order[i]].score * boost;
    }
    return new ScorerSupplier() {
      @Override
      public Scorer get(long leadCost) {
        return new Scorer() {
          private int i = -1;

          private final DocIdSetIterator it =
              new DocIdSetIterator() {
                @Override
                public int docID() {
                  return i < 0 ? -1 : (i >= n ? NO_MORE_DOCS : docs[i]);
                }

                @Override
                public int nextDoc() {
                  return ++i >= n ? NO_MORE_DOCS : docs[i];
                }

                @Override
                public int advance(int target) {
                  while (++i < n && docs[i] < target) {}
                  return i >= n ? NO_MORE_DOCS : docs[i];
                }

                @Override
                public long cost() {
                  return n;
                }
              };

          @Override
          public DocIdSetIterator iterator() {
            return it;
          }

          @Override
          public int docID() {
            return it.docID();
          }

          @Override
          public float score() {
            return scores[i];
          }

          @Override
          public float getMaxScore(int upTo) {
            return Float.MAX_VALUE;
          }
        };
      }

      @Override
      public long cost() {
        return n;
      }
    };
  }

  /**
   * The ivfaster reader behind {@code leaf} for this field; null when the leaf has no such vector
   * field or exposes no reachable codec (the caller then falls back; see {@link
   * #fallbackSupplier}); an error when the field exists but another format wrote it.
   */
  private IVFasterVectorsReader ivfasterReader(LeafReader leaf) {
    final var info = leaf.getFieldInfos().fieldInfo(field);
    if (info == null || info.getVectorDimension() == 0) {
      return null;
    }
    final LeafReader unwrapped = FilterLeafReader.unwrap(leaf);
    if (unwrapped instanceof CodecReader codecReader) {
      final KnnVectorsReader vectors = codecReader.getVectorReader();
      if (vectors != null
          && vectors.unwrapReaderForField(field) instanceof IVFasterVectorsReader r) {
        return r;
      }
      throw new IllegalStateException(
          "field '" + field + "' is not an ivfaster vector field in " + leaf);
    }
    // A leaf whose codec is not reachable: a ParallelLeafReader, or any other wrapper that is not a
    // FilterLeafReader and so cannot be unwrapped to a CodecReader. There are no cells to walk
    // here,
    // and returning null would make this clause silently match NOTHING -- which is what it used to
    // do, and is why TestIVFasterKnnQuery.testAgreesWithKnnFloatVectorQuery returned agreement 0.0
    // on any seed where the test framework wrapped the reader that way. The caller falls back to
    // the
    // standard LeafReader kNN API instead, which wrappers do delegate; see scorerSupplier.
    return null;
  }

  /**
   * The documents the probe admits, fine-scored on demand; the sizing that decides which and how
   * many lives in {@link #create}.
   *
   * <p>THE PROBE IS SIZED FROM {@code leadCost}. A conjunction hands the scorer the cost of the
   * clause that will lead it, which is the filter's cardinality when a filter is sparser than this
   * clause. A filter accepting {@code leadCost} documents lands about {@code leadCost / nlist} in
   * each cell, so reaching the codec's filtered target takes {@code target * nlist / leadCost}
   * cells; that many are selected, never fewer than the configured probe and never more than the
   * codec's cap. The coarse cut widens by the same ratio, so about {@code admit} ACCEPTED documents
   * survive it rather than {@code admit} rows of which the filter keeps a fraction. Below the
   * byte-parity bound the walk is dropped altogether: the approximation is every document with a
   * vector, in doc order, and every accepted document is fine-scored, since reranking them all
   * reads fewer bytes than the cells would. When this clause leads, {@code leadCost} is its own
   * cost and the configured probe and cut stand.
   *
   * <p>Admission is a FIXED cut computed when the scorer is built, never a threshold that tightens
   * as documents stream by: a conjunction may skip through this scorer in any order, and a
   * document's match must not depend on the path taken to reach it. That is also what lets the
   * admitted documents be materialized as a set instead of verified one at a time; see the two
   * subclasses, which differ only in what they iterate and where a document's slot comes from.
   */
  private abstract static class CellScorer extends Scorer {
    final IVFasterVectorsReader.CellSession session;
    private final float boost;
    private int scoredDoc = -1;
    private float score;

    static Scorer create(
        IVFasterVectorsReader.CellSession session,
        long fieldCost,
        long leadCost,
        int admit,
        float boost)
        throws IOException {
      if (leadCost < fieldCost && leadCost <= session.exactBound()) {
        return new ExactScorer(session, session.allDocs(), boost);
      }
      int probe = session.probe();
      // The codec's pool: admit slots for every copy a document may hold, then dedup to admit.
      long admitRows = (long) admit * session.fanout();
      // A leading clause sparser than the field is a filter, and its rarity is how much wider this
      // probe has to reach. Compared against the FIELD, not against this probe's own walk: the
      // question is whether a filter is restricting, which has nothing to do with nprobe.
      if (leadCost < fieldCost) {
        final double rarity = (double) fieldCost / Math.max(1, leadCost);
        // SCALE THE PROBE, do not re-derive it from a document count. The previous formula asked
        // "how many cells hold filteredTarget accepted documents" and answered 7 at 10%
        // selectivity, where 32 was already needed UNFILTERED -- so it never bound and the probe
        // never grew. The configured probe is the tuned operating point; rarity says how much
        // further it has to reach to hold the same recall.
        //
        // The exponent is measured, not assumed, and it depends on how many documents a cell holds
        // -- see rarityExponent. Sweeping nprobe to hold recall 0.95 on 1M/nlist=1000/spillBits=1
        // gives multipliers 1.41x / 1.70x / 2.42x / 3.09x at selectivity 50 / 25 / 10 / 5%. Linear
        // in rarity would ask for 20x where 3.09x holds; sqrt would ask 4.47x.
        final double k =
            IVFasterVectorsReader.rarityExponent(Math.max(1, session.count() / session.nlist()));
        probe =
            (int)
                Math.max(
                    probe,
                    Math.min(session.maxProbe(), (long) Math.ceil(probe * Math.pow(rarity, k))));
        admitRows = (long) Math.ceil(admitRows * rarity);
      }
      // Nothing admitted is not "no cells": an empty probed region matches no document, and a
      // scorer that yields none is exactly what the conjunction should get.
      if (session.prepare(probe, admitRows) == 0) {
        return null;
      }
      return new AdmittedScorer(session, session.admittedDocs(), boost);
    }

    CellScorer(IVFasterVectorsReader.CellSession session, float boost) {
      this.session = session;
      this.boost = boost;
    }

    /** The slot to fine-score the current document from. */
    abstract int slot();

    @Override
    public final int docID() {
      return iterator().docID();
    }

    @Override
    public final float score() throws IOException {
      final int doc = docID();
      if (doc != scoredDoc) {
        score = boost * session.fineScore(slot());
        scoredDoc = doc;
      }
      return score;
    }

    @Override
    public final float getMaxScore(int upTo) {
      return Float.POSITIVE_INFINITY;
    }
  }

  /**
   * The widened-probe plan: the documents {@code prepare} admitted, as one iterator.
   *
   * <p>NO VERIFICATION PHASE, so no {@link TwoPhaseIterator}. Admission was applied when the cut
   * was fixed, so every document this iterator yields matches, and there is nothing a second phase
   * could test. What that removes is not just the {@code matches()} call: it removes the
   * disjunction the approximation used to be — a heap over the probed cells plus a linear rescan of
   * whatever did not fit in it, per {@code advance} — which the profile put at 62% of query samples
   * with one filter clause and 74% with four. See {@code CellSession#admittedDocs}.
   */
  private static final class AdmittedScorer extends CellScorer {
    private final DocIdSetIterator docs;

    AdmittedScorer(IVFasterVectorsReader.CellSession session, DocIdSetIterator docs, float boost) {
      super(session, boost);
      this.docs = docs;
    }

    @Override
    public DocIdSetIterator iterator() {
      return docs;
    }

    @Override
    int slot() {
      return session.slotForDoc(docs.docID());
    }
  }

  /**
   * The exact plan: every document with a vector, in doc order, fine-scored as the conjunction
   * reaches it. For a filter narrow enough that reranking all it accepts reads fewer bytes than the
   * cells would; see {@code CellSession#exactBound}.
   *
   * <p>Also without a verification phase: this approximation admits everything on purpose, so a
   * second phase would only ever return true. The posting list's "slot" is an ordinal here.
   */
  private static final class ExactScorer extends CellScorer {
    private final CellPostings docs;

    ExactScorer(IVFasterVectorsReader.CellSession session, CellPostings docs, float boost) {
      super(session, boost);
      this.docs = docs;
    }

    @Override
    public DocIdSetIterator iterator() {
      return docs;
    }

    @Override
    int slot() {
      return session.ordToSlot(docs.slot());
    }
  }

  @Override
  public String toString(String field) {
    return "IVFasterKnnQuery(field="
        + this.field
        + ", dim="
        + target.length
        + ", nprobe="
        + nprobe
        + ", admit="
        + admit
        + ", adaptive="
        + adaptive
        + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo((IVFasterKnnQuery) other);
  }

  private boolean equalsTo(IVFasterKnnQuery o) {
    return field.equals(o.field)
        && Arrays.equals(target, o.target)
        && nprobe == o.nprobe
        && admit == o.admit
        && adaptive == o.adaptive
        // Sizing-only, but it changes the probe and therefore the matches, so two queries differing
        // only here are not interchangeable and must not share a cache entry.
        && Objects.equals(filterForSizing, o.filterForSizing)
        && Objects.equals(dependence, o.dependence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        classHash(),
        field,
        Arrays.hashCode(target),
        nprobe,
        admit,
        adaptive,
        filterForSizing,
        dependence);
  }
}
