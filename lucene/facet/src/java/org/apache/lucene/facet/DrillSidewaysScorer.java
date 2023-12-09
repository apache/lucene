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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;

class DrillSidewaysScorer extends BulkScorer {

  private static final Comparator<DocsAndCost> APPROXIMATION_COMPARATOR =
      Comparator.comparingLong(e -> e.cost);

  private static final Comparator<DocsAndCost> TWO_PHASE_COMPARATOR =
      (a, b) -> Float.compare(a.matchCost, b.matchCost);

  // private static boolean DEBUG = false;

  private final LeafCollector drillDownLeafCollector;

  private final DocsAndCost[] dims;

  // DrillDown DocsEnums:
  private final Scorer baseScorer;
  private final DocIdSetIterator baseIterator;
  private final DocIdSetIterator baseApproximation;
  private final TwoPhaseIterator baseTwoPhase;

  private final LeafReaderContext context;

  final boolean scoreSubDocsAtOnce;

  private static final int CHUNK = 2048;
  private static final int MASK = CHUNK - 1;

  private int collectDocID = -1;
  private float collectScore;

  DrillSidewaysScorer(
      LeafReaderContext context,
      Scorer baseScorer,
      LeafCollector drillDownLeafCollector,
      DocsAndCost[] dims,
      boolean scoreSubDocsAtOnce) {
    this.dims = dims;
    this.context = context;
    this.baseScorer = baseScorer;
    this.baseIterator = baseScorer.iterator();
    this.baseTwoPhase = baseScorer.twoPhaseIterator();
    if (baseTwoPhase != null) {
      this.baseApproximation = baseTwoPhase.approximation();
    } else {
      this.baseApproximation = baseIterator;
    }
    this.drillDownLeafCollector = drillDownLeafCollector;
    this.scoreSubDocsAtOnce = scoreSubDocsAtOnce;
  }

  @Override
  public long cost() {
    return baseIterator.cost();
  }

  @Override
  public int score(LeafCollector collector, Bits acceptDocs, int min, int maxDoc)
      throws IOException {
    if (min != 0) {
      throw new IllegalArgumentException("min must be 0, got " + min);
    }
    if (maxDoc != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("maxDoc must be Integer.MAX_VALUE");
    }

    // some scorers, eg ReqExlScorer, can hit NPE if cost is called after nextDoc
    long baseQueryCost = baseIterator.cost();

    final int numDims = dims.length;

    long drillDownCost = 0;
    for (int dim = 0; dim < numDims; dim++) {
      drillDownCost += dims[dim].approximation.cost();
    }

    long drillDownAdvancedCost = 0;
    if (numDims > 1) {
      drillDownAdvancedCost = dims[1].approximation.cost();
    }

    // Position dims scorers to their first matching doc:
    for (DocsAndCost dim : dims) {
      dim.approximation.nextDoc();
    }

    /*
    System.out.println("\nbaseDocID=" + baseScorer.docID() + " est=" + estBaseHitCount);
    System.out.println("  maxDoc=" + context.reader().maxDoc());
    System.out.println("  maxCost=" + maxCost);
    System.out.println("  dims[0].freq=" + dims[0].freq);
    if (numDims > 1) {
      System.out.println("  dims[1].freq=" + dims[1].freq);
    }
    */

    try {
      if (scoreSubDocsAtOnce || baseQueryCost < drillDownCost / 10) {
        // System.out.println("queryFirst: baseScorer=" + baseScorer + " disis.length=" +
        // disis.length
        // + " bits.length=" + bits.length);
        // position base scorer to the first matching doc
        baseApproximation.nextDoc();
        doQueryFirstScoring(acceptDocs, collector, dims);
      } else if (numDims > 1 && drillDownAdvancedCost < baseQueryCost / 10) {
        // System.out.println("drillDownAdvance");
        // position base scorer to the first matching doc
        baseIterator.nextDoc();
        doDrillDownAdvanceScoring(acceptDocs, collector, dims);
      } else {
        // System.out.println("union");
        // position base scorer to the first matching doc
        baseIterator.nextDoc();
        doUnionScoring(acceptDocs, collector, dims);
      }
    } finally {
      // TODO: What's the right behavior when a collector throws CollectionTerminatedException?
      // Should we stop scoring immediately (what we're doing now), or should we keep scoring until
      // all collectors throw? Should users be able to specify somehow?
      finish(dims);
    }

    return Integer.MAX_VALUE;
  }

  /**
   * Query-first scoring specialization when there is only one drill-sideways dimension, which is
   * likely a common scenario.
   */
  private void doQueryFirstScoringSingleDim(
      Bits acceptDocs, LeafCollector collector, DocsAndCost dim) throws IOException {
    int docID = baseApproximation.docID();
    while (docID != DocIdSetIterator.NO_MORE_DOCS) {
      assert docID == baseApproximation.docID();

      if (acceptDocs != null && acceptDocs.get(docID) == false) {
        docID = baseApproximation.nextDoc();
        continue;
      }

      if (baseTwoPhase != null && baseTwoPhase.matches() == false) {
        docID = baseApproximation.nextDoc();
        continue;
      }

      // We have either a near-miss or full match. Check the sideways dim to see which it is:
      collectDocID = docID;
      if (advanceIfBehind(docID, dim.approximation) != docID
          || (dim.twoPhase != null && dim.twoPhase.matches() == false)) {
        // The sideways dim missed, so we have a "near miss":
        collectNearMiss(dim.sidewaysLeafCollector);
      } else {
        // Hit passed all filters, so it's "real":
        collectHit(collector, dim);
      }

      docID = baseApproximation.nextDoc();
    }
  }

  /**
   * Used when base query is highly constraining vs the drilldowns, or when the docs must be scored
   * at once (i.e., like BooleanScorer2, not BooleanScorer).
   */
  private void doQueryFirstScoring(Bits acceptDocs, LeafCollector collector, DocsAndCost[] dims)
      throws IOException {
    setScorer(collector, ScoreCachingWrappingScorer.wrap(baseScorer));

    // Specialize the single-dim use-case as we have a more efficient implementation for that:
    if (dims.length == 1) {
      doQueryFirstScoringSingleDim(acceptDocs, collector, dims[0]);
      return;
    }

    // Sort our sideways dims by approximation cost so we can advance the lower cost ones first:
    List<DocsAndCost> sidewaysDims = new ArrayList<>(dims.length);
    sidewaysDims.addAll(List.of(dims));
    CollectionUtil.timSort(sidewaysDims, APPROXIMATION_COMPARATOR);

    // Maintain (optional) subset of sideways dims that support two-phase iteration, sorted by
    // matchCost:
    List<DocsAndCost> sidewaysTwoPhaseDims = null;
    for (DocsAndCost dim : dims) {
      if (dim.twoPhase != null) {
        if (sidewaysTwoPhaseDims == null) {
          sidewaysTwoPhaseDims = new ArrayList<>();
        }
        sidewaysTwoPhaseDims.add(dim);
      }
    }
    if (sidewaysTwoPhaseDims != null) {
      CollectionUtil.timSort(sidewaysTwoPhaseDims, TWO_PHASE_COMPARATOR);
    }

    // We keep track of a "runaway" dimension, which is a previously "near missed" dimension that
    // has advanced beyond the docID the rest of the dimensions are positioned on. This functions
    // a bit like the "head" queue in WANDScorer's "min should match" implementation. We use a
    // single-valued PQ ordered by docID to easily determine the "closest" runaway dim we'll use
    // for advancing in the case that multiple dim approximations miss.
    PriorityQueue<DocsAndCost> runawayDim =
        new PriorityQueue<>(1) {
          @Override
          protected boolean lessThan(DocsAndCost a, DocsAndCost b) {
            return a.approximation.docID() < b.approximation.docID();
          }
        };

    int docID = baseApproximation.docID();

    nextDoc:
    while (docID != DocIdSetIterator.NO_MORE_DOCS) {
      assert docID == baseApproximation.docID();

      if (acceptDocs != null && acceptDocs.get(docID) == false) {
        docID = baseApproximation.nextDoc();
        continue;
      }

      // If we carried a "runaway" over from the last iteration, see if we've "caught up" yet:
      DocsAndCost runaway = runawayDim.top();
      if (runaway != null && runaway.approximation.docID() <= docID) {
        runawayDim.clear();
        runaway = null;
      }

      // Check the sideways dim approximations. At most, one dim is allowed to miss for the doc
      // to be a near-miss or full match. If multiple sideways dims miss, we move on:
      for (DocsAndCost dim : sidewaysDims) {
        int dimDocID = advanceIfBehind(docID, dim.approximation);
        if (dimDocID != docID && dim != runaway) {
          DocsAndCost evicted = runawayDim.insertWithOverflow(dim);
          if (evicted != null) {
            // More than one dim has advanced beyond docID, so we jump ahead to the "closer" of
            // the two:
            int next = evicted.approximation.docID();
            docID = baseApproximation.advance(next);
            continue nextDoc;
          }
        }
      }

      // At this point, we have an "approximate" near-miss or full match, but we still need
      // to confirm two-phase iterators. First, check the base two-phase (it's always required):
      if (baseTwoPhase != null && baseTwoPhase.matches() == false) {
        docID = baseApproximation.nextDoc();
        continue;
      }

      // If we have two-phase iterators for our sideways dims, check them now. At most, one
      // sideways dim can miss for the doc to be a near-miss or full match. If more than one misses
      // we move on:
      DocsAndCost failedDim = runawayDim.top();
      if (sidewaysTwoPhaseDims != null) {
        if (failedDim == null) {
          // If all sideways dims matched in their approximation phase, then we can allow one
          // second-phase check to fail:
          for (DocsAndCost dim : sidewaysTwoPhaseDims) {
            assert dim.approximation.docID() == docID;
            if (dim.twoPhase.matches() == false) {
              if (failedDim != null) {
                // Two second-phase checks have failed, so we move on:
                docID = baseApproximation.nextDoc();
                continue nextDoc;
              } else {
                failedDim = dim;
              }
            }
          }
        } else {
          // If a sideways dim failed the approximate check, then no second-phase checks can fail:
          for (DocsAndCost dim : sidewaysTwoPhaseDims) {
            if (failedDim == dim) {
              continue;
            }
            assert dim.approximation.docID() == docID;
            if (dim.twoPhase.matches() == false) {
              docID = baseApproximation.nextDoc();
              continue nextDoc;
            }
          }
        }
      }

      collectDocID = docID;
      if (failedDim == null) {
        // Hit passed all filters, so it's "real":
        collectHit(collector, sidewaysDims);
      } else {
        // Hit missed exactly one dim:
        collectNearMiss(failedDim.sidewaysLeafCollector);
      }

      docID = baseApproximation.nextDoc();
    }
  }

  private static int advanceIfBehind(int docID, DocIdSetIterator iterator) throws IOException {
    if (iterator.docID() < docID) {
      return iterator.advance(docID);
    } else {
      return iterator.docID();
    }
  }

  /** Used when drill downs are highly constraining vs baseQuery. */
  private void doDrillDownAdvanceScoring(
      Bits acceptDocs, LeafCollector collector, DocsAndCost[] dims) throws IOException {
    setScorer(collector, new ScoreAndDoc());

    final int maxDoc = context.reader().maxDoc();
    final int numDims = dims.length;

    // if (DEBUG) {
    //  System.out.println("  doDrillDownAdvanceScoring");
    // }

    // TODO: maybe a class like BS, instead of parallel arrays
    int[] filledSlots = new int[CHUNK];
    int[] docIDs = new int[CHUNK];
    float[] scores = new float[CHUNK];
    int[] missingDims = new int[CHUNK];
    int[] counts = new int[CHUNK];

    docIDs[0] = -1;
    int nextChunkStart = CHUNK;

    final FixedBitSet seen = new FixedBitSet(CHUNK);

    while (true) {
      // if (DEBUG) {
      //  System.out.println("\ncycle nextChunkStart=" + nextChunkStart + " docIds[0]=" +
      // docIDs[0]);
      // }

      // First dim:
      // if (DEBUG) {
      //  System.out.println("  dim0");
      // }
      DocsAndCost dc = dims[0];
      int docID = dc.approximation.docID();
      while (docID < nextChunkStart) {
        if (acceptDocs == null || acceptDocs.get(docID)) {
          int slot = docID & MASK;

          if (docIDs[slot] != docID && (dc.twoPhase == null || dc.twoPhase.matches())) {
            seen.set(slot);
            // Mark slot as valid:
            // if (DEBUG) {
            //  System.out.println("    set docID=" + docID + " id=" +
            // context.reader().document(docID).get("id"));
            // }
            docIDs[slot] = docID;
            missingDims[slot] = 1;
            counts[slot] = 1;
          }
        }

        docID = dc.approximation.nextDoc();
      }

      // Second dim:
      // if (DEBUG) {
      //  System.out.println("  dim1");
      // }
      dc = dims[1];
      docID = dc.approximation.docID();
      while (docID < nextChunkStart) {
        if (acceptDocs == null
            || acceptDocs.get(docID) && (dc.twoPhase == null || dc.twoPhase.matches())) {
          int slot = docID & MASK;

          if (docIDs[slot] != docID) {
            // Mark slot as valid:
            seen.set(slot);
            // if (DEBUG) {
            //  System.out.println("    set docID=" + docID + " missingDim=0 id=" +
            // context.reader().document(docID).get("id"));
            // }
            docIDs[slot] = docID;
            missingDims[slot] = 0;
            counts[slot] = 1;
          } else {
            // TODO: single-valued dims will always be true
            // below; we could somehow specialize
            if (missingDims[slot] >= 1) {
              missingDims[slot] = 2;
              counts[slot] = 2;
              // if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " missingDim=2 id=" +
              // context.reader().document(docID).get("id"));
              // }
            } else {
              counts[slot] = 1;
              // if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " missingDim=" + missingDims[slot] +
              // " id=" + context.reader().document(docID).get("id"));
              // }
            }
          }
        }

        docID = dc.approximation.nextDoc();
      }

      // After this we can "upgrade" to conjunction, because
      // any doc not seen by either dim 0 or dim 1 cannot be
      // a hit or a near miss:

      // if (DEBUG) {
      //  System.out.println("  baseScorer");
      // }

      // Fold in baseScorer, using advance:
      int filledCount = 0;
      int slot0 = 0;
      while (slot0 < CHUNK && (slot0 = seen.nextSetBit(slot0)) != DocIdSetIterator.NO_MORE_DOCS) {
        int ddDocID = docIDs[slot0];
        assert ddDocID != -1;

        int baseDocID = baseIterator.docID();
        if (baseDocID < ddDocID) {
          baseDocID = baseIterator.advance(ddDocID);
        }
        if (baseDocID == ddDocID) {
          // if (DEBUG) {
          //  System.out.println("    keep docID=" + ddDocID + " id=" +
          // context.reader().document(ddDocID).get("id"));
          // }
          scores[slot0] = baseScorer.score();
          filledSlots[filledCount++] = slot0;
          counts[slot0]++;
        } else {
          // if (DEBUG) {
          //  System.out.println("    no docID=" + ddDocID + " id=" +
          // context.reader().document(ddDocID).get("id"));
          // }
          docIDs[slot0] = -1;

          // TODO: we could jump slot0 forward to the
          // baseDocID ... but we'd need to set docIDs for
          // intervening slots to -1
        }
        slot0++;
      }
      seen.clear(0, CHUNK);

      if (filledCount == 0) {
        if (nextChunkStart >= maxDoc) {
          break;
        }
        nextChunkStart += CHUNK;
        continue;
      }

      // TODO: factor this out & share w/ union scorer,
      // except we start from dim=2 instead:
      for (int dim = 2; dim < numDims; dim++) {
        // if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        // }
        dc = dims[dim];
        docID = dc.approximation.docID();
        while (docID < nextChunkStart) {
          int slot = docID & MASK;
          if (docIDs[slot] == docID
              && counts[slot] >= dim
              && (dc.twoPhase == null || dc.twoPhase.matches())) {
            // TODO: single-valued dims will always be true
            // below; we could somehow specialize
            if (missingDims[slot] >= dim) {
              // if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " count=" + (dim+2));
              // }
              missingDims[slot] = dim + 1;
              counts[slot] = dim + 2;
            } else {
              // if (DEBUG) {
              //  System.out.println("    set docID=" + docID + " missing count=" + (dim+1));
              // }
              counts[slot] = dim + 1;
            }
          }

          // TODO: sometimes use advance?
          docID = dc.approximation.nextDoc();
        }
      }

      // Collect:
      // if (DEBUG) {
      //  System.out.println("  now collect: " + filledCount + " hits");
      // }
      for (int i = 0; i < filledCount; i++) {
        int slot = filledSlots[i];
        collectDocID = docIDs[slot];
        collectScore = scores[slot];
        // if (DEBUG) {
        //  System.out.println("    docID=" + docIDs[slot] + " count=" + counts[slot]);
        // }
        if (counts[slot] == 1 + numDims) {
          collectHit(collector, dims);
        } else if (counts[slot] == numDims) {
          collectNearMiss(dims[missingDims[slot]].sidewaysLeafCollector);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  private void doUnionScoring(Bits acceptDocs, LeafCollector collector, DocsAndCost[] dims)
      throws IOException {
    // if (DEBUG) {
    //  System.out.println("  doUnionScoring");
    // }
    setScorer(collector, new ScoreAndDoc());

    final int maxDoc = context.reader().maxDoc();
    final int numDims = dims.length;

    // TODO: maybe a class like BS, instead of parallel arrays
    int[] filledSlots = new int[CHUNK];
    int[] docIDs = new int[CHUNK];
    float[] scores = new float[CHUNK];
    int[] missingDims = new int[CHUNK];
    int[] counts = new int[CHUNK];

    docIDs[0] = -1;

    // NOTE: this is basically a specialized version of
    // BooleanScorer, to the minShouldMatch=N-1 case, but
    // carefully tracking which dimension failed to match

    int nextChunkStart = CHUNK;

    while (true) {
      // if (DEBUG) {
      //  System.out.println("\ncycle nextChunkStart=" + nextChunkStart + " docIds[0]=" +
      // docIDs[0]);
      // }
      int filledCount = 0;
      int docID = baseIterator.docID();

      // if (DEBUG) {
      //  System.out.println("  base docID=" + docID);
      // }
      while (docID < nextChunkStart) {
        if (acceptDocs == null || acceptDocs.get(docID)) {
          int slot = docID & MASK;
          // if (DEBUG) {
          //  System.out.println("    docIDs[slot=" + slot + "]=" + docID + " id=" +
          // context.reader().document(docID).get("id"));
          // }

          // Mark slot as valid:
          assert docIDs[slot] != docID : "slot=" + slot + " docID=" + docID;
          docIDs[slot] = docID;
          scores[slot] = baseScorer.score();
          filledSlots[filledCount++] = slot;
          missingDims[slot] = 0;
          counts[slot] = 1;
        }
        docID = baseIterator.nextDoc();
      }

      if (filledCount == 0) {
        if (nextChunkStart >= maxDoc) {
          break;
        }
        nextChunkStart += CHUNK;
        continue;
      }

      // First drill-down dim, basically adds SHOULD onto
      // the baseQuery:
      // if (DEBUG) {
      //  System.out.println("  dim=0 [" + dims[0].dim + "]");
      // }
      {
        DocsAndCost dc = dims[0];
        docID = dc.approximation.docID();
        // if (DEBUG) {
        //  System.out.println("    start docID=" + docID);
        // }
        while (docID < nextChunkStart) {
          int slot = docID & MASK;
          if (docIDs[slot] == docID // this also checks that the doc is not deleted
              && (dc.twoPhase == null || dc.twoPhase.matches())) {
            // if (DEBUG) {
            //  System.out.println("      set docID=" + docID + " count=2");
            // }
            missingDims[slot] = 1;
            counts[slot] = 2;
          }
          docID = dc.approximation.nextDoc();
        }
      }

      for (int dim = 1; dim < numDims; dim++) {
        // if (DEBUG) {
        //  System.out.println("  dim=" + dim + " [" + dims[dim].dim + "]");
        // }

        DocsAndCost dc = dims[dim];
        docID = dc.approximation.docID();
        // if (DEBUG) {
        //  System.out.println("    start docID=" + docID);
        // }
        while (docID < nextChunkStart) {
          int slot = docID & MASK;
          if (docIDs[slot] == docID // also means that the doc is not deleted
              && counts[slot] >= dim
              && (dc.twoPhase == null || dc.twoPhase.matches())) {
            // This doc is still in the running...
            // TODO: single-valued dims will always be true
            // below; we could somehow specialize
            if (missingDims[slot] >= dim) {
              // if (DEBUG) {
              //  System.out.println("      set docID=" + docID + " count=" + (dim+2));
              // }
              missingDims[slot] = dim + 1;
              counts[slot] = dim + 2;
            } else {
              // if (DEBUG) {
              //  System.out.println("      set docID=" + docID + " missing count=" + (dim+1));
              // }
              counts[slot] = dim + 1;
            }
          }
          docID = dc.approximation.nextDoc();
        }
      }

      // Collect:
      // System.out.println("  now collect: " + filledCount + " hits");
      for (int i = 0; i < filledCount; i++) {
        // NOTE: This is actually in-order collection,
        // because we only accept docs originally returned by
        // the baseScorer (ie that Scorer is AND'd)
        int slot = filledSlots[i];
        collectDocID = docIDs[slot];
        collectScore = scores[slot];
        // if (DEBUG) {
        //  System.out.println("    docID=" + docIDs[slot] + " count=" + counts[slot]);
        // }
        // System.out.println("  collect doc=" + collectDocID + " main.freq=" + (counts[slot]-1) + "
        // main.doc=" + collectDocID + " exactCount=" + numDims);
        if (counts[slot] == 1 + numDims) {
          // System.out.println("    hit");
          collectHit(collector, dims);
        } else if (counts[slot] == numDims) {
          // System.out.println("    sw");
          collectNearMiss(dims[missingDims[slot]].sidewaysLeafCollector);
        }
      }

      if (nextChunkStart >= maxDoc) {
        break;
      }

      nextChunkStart += CHUNK;
    }
  }

  private void collectHit(LeafCollector collector, DocsAndCost[] dims) throws IOException {
    // if (DEBUG) {
    //  System.out.println("      hit");
    // }

    collector.collect(collectDocID);
    if (drillDownLeafCollector != null) {
      drillDownLeafCollector.collect(collectDocID);
    }

    // TODO: we could "fix" faceting of the sideways counts
    // to do this "union" (of the drill down hits) in the
    // end instead:

    // Tally sideways counts:
    for (DocsAndCost dim : dims) {
      dim.sidewaysLeafCollector.collect(collectDocID);
    }
  }

  private void collectHit(LeafCollector collector, DocsAndCost dim) throws IOException {
    collector.collect(collectDocID);
    if (drillDownLeafCollector != null) {
      drillDownLeafCollector.collect(collectDocID);
    }

    // Tally sideways count:
    dim.sidewaysLeafCollector.collect(collectDocID);
  }

  private void collectHit(LeafCollector collector, List<DocsAndCost> dims) throws IOException {
    collector.collect(collectDocID);
    if (drillDownLeafCollector != null) {
      drillDownLeafCollector.collect(collectDocID);
    }

    // Tally sideways counts:
    for (DocsAndCost dim : dims) {
      dim.sidewaysLeafCollector.collect(collectDocID);
    }
  }

  private void collectNearMiss(LeafCollector sidewaysCollector) throws IOException {
    // if (DEBUG) {
    //  System.out.println("      missingDim=" + dim);
    // }
    sidewaysCollector.collect(collectDocID);
  }

  private void finish(DocsAndCost[] dims) throws IOException {
    // Note: We _only_ call #finish on the facets collectors we're managing here, but not the
    // "main" collector. This is because IndexSearcher handles calling #finish on the main
    // collector.
    if (drillDownLeafCollector != null) {
      drillDownLeafCollector.finish();
    }
    for (DocsAndCost dim : dims) {
      dim.sidewaysLeafCollector.finish();
    }
  }

  private void setScorer(LeafCollector mainCollector, Scorable scorer) throws IOException {
    mainCollector.setScorer(scorer);
    if (drillDownLeafCollector != null) {
      drillDownLeafCollector.setScorer(scorer);
    }
    for (DocsAndCost dim : dims) {
      dim.sidewaysLeafCollector.setScorer(scorer);
    }
  }

  private final class ScoreAndDoc extends Scorable {

    @Override
    public int docID() {
      return collectDocID;
    }

    @Override
    public float score() {
      return collectScore;
    }

    @Override
    public Collection<ChildScorable> getChildren() {
      return Collections.singletonList(new ChildScorable(baseScorer, "MUST"));
    }
  }

  static class DocsAndCost {
    // approximation of matching docs, or the scorer itself
    final DocIdSetIterator approximation;
    final long cost;
    // two-phase confirmation, or null if the approximation is accurate
    final TwoPhaseIterator twoPhase;
    final float matchCost;
    final LeafCollector sidewaysLeafCollector;

    DocsAndCost(Scorer scorer, LeafCollector sidewaysLeafCollector) {
      final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
      if (twoPhase == null) {
        this.approximation = scorer.iterator();
        this.twoPhase = null;
        this.matchCost = 0f;
      } else {
        this.approximation = twoPhase.approximation();
        this.twoPhase = twoPhase;
        this.matchCost = twoPhase.matchCost();
      }
      this.cost = approximation.cost();
      this.sidewaysLeafCollector = sidewaysLeafCollector;
    }
  }
}
