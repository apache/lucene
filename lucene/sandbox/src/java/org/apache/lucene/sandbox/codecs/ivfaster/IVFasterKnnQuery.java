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
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
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
 * the way a search does, and its scorer's iterator is the DISJUNCTION of those cells' posting lists
 * ({@link CellPostings}), in doc order. Inside a conjunction the sparser clause leads, and the
 * cells are advanced to whatever it yields.
 *
 * <p>Scoring is two-phase, so the expensive tier is paid only for documents every clause agrees on.
 * {@code matches()} is COARSE ADMISSION against a cut fixed when the scorer is built: the probed
 * runs are Hamming-scored in bulk and the cut is the coarse distance that admits {@code admit}
 * rows, the same histogram cut the codec's own scan takes. {@code score()} is the fine tier, one
 * record read and one int8 dot, computed only for documents the whole conjunction yields.
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
        if (scorer != null) {
          final TwoPhaseIterator tp = scorer.twoPhaseIterator();
          if (tp.approximation().advance(doc) == doc && tp.matches()) {
            return Explanation.match(
                scorer.score(), "ivfaster fine score of doc " + doc + " in field " + field);
          }
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
   * Estimated size of {@link #filterForSizing}'s intersection on this leaf, or {@code -1} if it
   * cannot be estimated.
   *
   * <p>Clauses are flattened out of nested pure conjunctions and their MARGINAL costs multiplied as
   * independent probabilities, then scaled by {@link #FILTER_CONJUNCTION_CORRECTOR}. A query
   * carrying {@code SHOULD} or {@code MUST_NOT} is not a conjunction and is left to report its own
   * cost, since a disjunction's cost is a sum and multiplying it would be nonsense.
   *
   * <p>Cost only — no iterator is pulled, so this walks no postings and reads no documents. It is
   * the same {@code cost()} the conjunction itself uses to order clauses.
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
    double selectivity = 1.0;
    for (Query clause : clauses) {
      final Weight w =
          searcher.createWeight(searcher.rewrite(clause), ScoreMode.COMPLETE_NO_SCORES, 1f);
      final ScorerSupplier ss = w.scorerSupplier(context);
      if (ss == null) {
        return 0; // a clause matches nothing on this leaf, so the conjunction does too
      }
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
   * The cells' union, coarse-admitted two-phase, fine-scored on demand.
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
   * document's match must not depend on the path taken to reach it.
   */
  private static final class CellScorer extends Scorer {
    private final IVFasterVectorsReader.CellSession session;
    private final DisjunctionDISIApproximation cells;
    private final DocIdSetIterator iterator;
    private final TwoPhaseIterator twoPhase;
    private final float boost;
    private int scoredDoc = -1;
    private int slot;
    private float score;

    static Scorer create(
        IVFasterVectorsReader.CellSession session,
        long fieldCost,
        long leadCost,
        int admit,
        float boost)
        throws IOException {
      final List<DisiWrapper> wrappers = new ArrayList<>();
      final boolean exact;
      if (leadCost < fieldCost && leadCost <= session.exactBound()) {
        exact = true;
        wrappers.add(new CellPostings.Wrapper(session.allDocs()));
      } else {
        exact = false;
        int probe = session.probe();
        // The codec's pool: admit slots for every copy a document may hold, then dedup to admit.
        long admitRows = (long) admit * session.fanout();
        // A leading clause sparser than the field is a filter, and its rarity is how much wider
        // this
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
          // The exponent is measured, not assumed, and it depends on how many documents a cell
          // holds
          // -- see rarityExponent. Sweeping nprobe to hold recall 0.95 on 1M/nlist=1000/spillBits=1
          // gives multipliers 1.41x / 1.70x / 2.42x / 3.09x at selectivity 50 / 25 / 10 / 5%.
          // Linear
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
        final int n = session.prepare(probe, admitRows);
        for (int i = 0; i < n; i++) {
          final CellPostings cell = session.postings(i);
          if (cell != null) {
            wrappers.add(new CellPostings.Wrapper(cell));
          }
        }
      }
      if (wrappers.isEmpty()) {
        return null;
      }
      return new CellScorer(session, wrappers, exact, leadCost, boost);
    }

    private CellScorer(
        IVFasterVectorsReader.CellSession session,
        List<DisiWrapper> wrappers,
        boolean exact,
        long leadCost,
        float boost) {
      this.session = session;
      this.cells = DisjunctionDISIApproximation.of(wrappers, leadCost);
      this.boost = boost;
      final int matchCost = exact ? 0 : session.coarseBytes();
      this.twoPhase =
          new TwoPhaseIterator(cells) {
            @Override
            public boolean matches() {
              // Every copy of a document carries the same code; the first is enough.
              final CellPostings cell = ((CellPostings.Wrapper) cells.topList()).cell;
              if (exact) {
                // The "slot" of the all-docs list is an ordinal.
                slot = session.ordToSlot(cell.slot());
                return true;
              }
              if (session.admitted(cell) == false) {
                return false;
              }
              slot = cell.slot();
              return true;
            }

            @Override
            public float matchCost() {
              return matchCost;
            }
          };
      this.iterator = TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    }

    @Override
    public int docID() {
      return cells.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return twoPhase;
    }

    @Override
    public float score() throws IOException {
      final int doc = cells.docID();
      if (doc != scoredDoc) {
        score = boost * session.fineScore(slot);
        scoredDoc = doc;
      }
      return score;
    }

    @Override
    public float getMaxScore(int upTo) {
      return Float.POSITIVE_INFINITY;
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
        && Objects.equals(filterForSizing, o.filterForSizing);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        classHash(), field, Arrays.hashCode(target), nprobe, admit, adaptive, filterForSizing);
  }
}
