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
          return null;
        }
        final IVFasterVectorsReader.CellSession session =
            reader.openCellSession(field, target, nprobe);
        if (session == null) {
          return null;
        }
        // What the configured probe would walk, before any cell is chosen; the conjunction reads
        // it to decide which clause leads.
        final long ownCost = Math.max(1, session.slots() * session.probe() / session.nlist());
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return CellScorer.create(
                session, ownCost, adaptive ? leadCost : Long.MAX_VALUE, admit, boost);
          }

          @Override
          public long cost() {
            return ownCost;
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
   * The ivfaster reader behind {@code leaf} for this field; null when the leaf has no such vector
   * field or is not a segment (no matches); an error when the field exists but another format wrote
   * it.
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
    // Not a segment (a synthetic or composite leaf): nothing to walk.
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
        long ownCost,
        long leadCost,
        int admit,
        float boost)
        throws IOException {
      final List<DisiWrapper> wrappers = new ArrayList<>();
      final boolean exact;
      if (leadCost < ownCost && leadCost <= session.exactBound()) {
        exact = true;
        wrappers.add(new CellPostings.Wrapper(session.allDocs()));
      } else {
        exact = false;
        int probe = session.probe();
        // The codec's pool: admit slots for every copy a document may hold, then dedup to admit.
        long admitRows = (long) admit * session.fanout();
        if (leadCost < ownCost) {
          final double widen = (double) session.count() / Math.max(1, leadCost);
          final long needed =
              (long)
                  Math.ceil(
                      session.filteredTarget() * session.nlist() / (double) Math.max(1, leadCost));
          probe = (int) Math.max(probe, Math.min(session.maxProbe(), needed));
          admitRows = (long) Math.ceil(admitRows * widen);
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
