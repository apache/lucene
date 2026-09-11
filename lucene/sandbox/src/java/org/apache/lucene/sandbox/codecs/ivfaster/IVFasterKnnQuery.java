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

  /**
   * How filter rarity scales the probe: {@code probe * rarity^k}; see {@code CellScorer.create}.
   *
   * <p>MEASURED, AND GEOMETRY-DEPENDENT. 0.383 is a least-squares fit over selectivity 5-50% on a
   * 1M segment at {@code nlist=1000} with {@code spillBits=1}, where a cell holds about a thousand
   * documents. At a much larger {@code nlist} a cell holds a handful instead, and the exponent that
   * preserves recall will differ, which is why this is a property rather than a constant. Uniform
   * random accept sets only: a filter correlated with the query breaks the premise that rarity
   * alone predicts how much further the probe must reach.
   */
  private static final double FILTER_RARITY_EXPONENT =
      Double.parseDouble(System.getProperty("ivfaster.filterRarityExponent", "0.383"));

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
  }

  /** As above, adaptive, with the segment's persisted {@code nprobe} and the codec's width. */
  public IVFasterKnnQuery(String field, float[] target) {
    this(field, target, 0, 0, true);
  }

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
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return CellScorer.create(
                session, fieldCost, adaptive ? leadCost : Long.MAX_VALUE, admit, boost);
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
          // The exponent is measured, not assumed. Sweeping nprobe to hold recall 0.95 on
          // 1M/nlist=1000/spillBits=1 gives multipliers 1.41x / 1.70x / 2.42x / 3.09x at
          // selectivity 50 / 25 / 10 / 5%, i.e. rarity^0.383 with three of the four points inside
          // 0.376-0.384. Linear in rarity would ask for 20x where 3.09x holds; sqrt would ask
          // 4.47x.
          probe =
              (int)
                  Math.max(
                      probe,
                      Math.min(
                          session.maxProbe(),
                          (long) Math.ceil(probe * Math.pow(rarity, FILTER_RARITY_EXPONENT))));
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
        && adaptive == o.adaptive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, Arrays.hashCode(target), nprobe, admit, adaptive);
  }
}
