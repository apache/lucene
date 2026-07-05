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

package org.apache.lucene.sandbox.search.knn;

import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.hnsw.FloatHeap;

/**
 * A {@link org.apache.lucene.search.KnnCollector.Decorator} that raises its {@link
 * #minCompetitiveSimilarity()} using a {@link GlobalKnnFloor} shared with the other searchers of
 * the same query, so that a graph search stops exploring candidates that cannot enter the query's
 * final, merged top-k, not merely candidates that cannot enter this collector's local top-k.
 *
 * <p>The graph searcher already re-reads {@link #minCompetitiveSimilarity()} whenever a collected
 * hit reports an improvement, and uses it both to stop the search when the best remaining candidate
 * falls below it and to avoid enqueueing hopeless candidates. Folding the shared floor into that
 * method is therefore the entire integration: no searcher changes are required, and a collector
 * whose floor never rises behaves exactly like its delegate.
 *
 * <p>Three guards keep the shared bound from harming recall or becoming a synchronization hot spot:
 *
 * <ul>
 *   <li><b>Ascent gate.</b> The shared floor is ignored until this collector has gathered {@code
 *       gateK} results, which by default is the size of its local queue. A graph search begins at
 *       an entry point that is usually far from the query, and every score observed while
 *       descending toward the query's neighborhood is uninformative; a bound derived from an
 *       already-converged sibling would terminate the search before it had a chance to find
 *       anything. Once the gate's worth of results has been gathered, the search has reached its
 *       neighborhood and competitiveness against the rest of the query is meaningful.
 *       <p>The gate may be set below the queue size when this collector's expected contribution to
 *       the merged result set is smaller than the queue it collects into. A searcher of one shard
 *       among many is statistically due only {@code k * proportion} of the query's final top-k, yet
 *       must be able to return more than that share when its shard actually holds more; gating at
 *       the expected share instead of at the queue size lets the search pay only the share's fill
 *       cost before the floor starts deciding whether continuing is worthwhile, while the full-size
 *       queue keeps every above-floor result the shard turns out to hold. Gating at the queue size
 *       in that configuration would force every shard to collect k results before pruning could
 *       begin, which costs more than not sharing a floor at all.
 *   <li><b>Greediness clamp.</b> Even after the gate opens, the effective bound is capped by the
 *       similarity of the {@code max(minExplorationSlots, (1 - greediness) * gateK)}-th best score
 *       this collector has seen. A search that is globally non-competitive is thus throttled rather
 *       than stopped outright: it keeps following its most promising frontier, which preserves the
 *       paths through mediocre intermediate nodes that graph navigation depends on. At {@code
 *       greediness = 0} the floor has no effect; at {@code greediness = 1} the collector retains
 *       only the absolute minimum of exploration slots. See {@link #DEFAULT_MIN_EXPLORATION_SLOTS}
 *       for why the clamp has an absolute lower bound.
 *   <li><b>Batched synchronization.</b> Scores are published to the shared floor, and the floor is
 *       re-read, only when the local queue first fills and every {@code syncInterval} visited
 *       vectors afterwards (default {@value #DEFAULT_SYNC_INTERVAL}), so the shared state is
 *       touched a constant number of times per few hundred scored candidates rather than once per
 *       candidate. A stale floor can only delay termination, never cause a wrong result, so the
 *       interval trades a bounded amount of extra work for the absence of cross-thread traffic in
 *       the scoring loop.
 * </ul>
 *
 * <p>The effective bound derived from the floor is one ulp below the floor itself. The floor is the
 * similarity of a real collected hit, and when several hits tie at the cutoff the merged result set
 * is decided by tie-breaking, not by score; a search must therefore remain willing to find
 * candidates exactly at the floor, otherwise a document that would have won the tie-break could be
 * abandoned. Keeping the bound strictly below the floor preserves those ties without publishing
 * document identities.
 *
 * <p>Instances are confined to a single thread, like every {@link KnnCollector}; only the shared
 * {@link GlobalKnnFloor} is touched by multiple threads.
 *
 * @lucene.experimental
 */
public final class FloorAwareKnnCollector extends KnnCollector.Decorator {

  /**
   * Default fraction of the search effort that follows the shared floor rather than the local
   * frontier; see the class comment for the roles of the two extremes. The default is deliberately
   * conservative. Callers who have verified recall on their own data may trade some of it for fewer
   * visits by raising this.
   */
  public static final float DEFAULT_GREEDINESS = 0.5f;

  /**
   * Default minimum number of non-competitive queue slots, whatever the greediness. The protection
   * a graph search needs against a tight external bound is an absolute number of below-bound
   * candidates it may keep routing through, not a fraction of k: with a purely fractional clamp, a
   * small-k search under high greediness is left a clamp one or two candidates wide, and randomized
   * testing showed that costing double-digit recall. A useful side effect is that when k does not
   * exceed the configured minimum, the clamp is at least as wide as the local queue and the shared
   * floor is neutralized entirely: small-k searches behave exactly like stock search no matter what
   * has been advertised.
   */
  public static final int DEFAULT_MIN_EXPLORATION_SLOTS = 16;

  /**
   * Default number of visited vectors between synchronizations with the shared floor. The value
   * balances floor freshness (a staler floor prunes less, costing visits but never recall) against
   * cross-thread traffic; it is the interval the removed {@code MultiLeafKnnCollector} shipped
   * with.
   */
  public static final int DEFAULT_SYNC_INTERVAL = 256;

  private final AbstractKnnCollector subCollector;
  private final GlobalKnnFloor globalFloor;

  /**
   * Number of locally collected results after which the shared floor engages; the ascent gate's
   * threshold. See the class comment.
   */
  private final int gateK;

  /**
   * Bit mask selecting the visited counts at which to synchronize, {@code syncInterval - 1}; the
   * power-of-two requirement on the interval exists so this mask works.
   */
  private final int syncIntervalMask;

  /**
   * The best {@code max(minExplorationSlots, (1 - greediness) * k)} similarities seen by this
   * collector, competitive or not. Its minimum caps the effective bound, implementing the
   * greediness clamp.
   */
  private final FloatHeap nonCompetitiveQueue;

  /** Similarities observed since the last synchronization, awaiting publication. */
  private final FloatHeap updatesQueue;

  /** Scratch used to drain {@link #updatesQueue} in ascending order for batch publication. */
  private final float[] updatesScratch;

  private boolean gateOpened;
  private float cachedGlobalFloor = Float.NEGATIVE_INFINITY;

  /**
   * Create a collector applying the {@link #DEFAULT_GREEDINESS default greediness}.
   *
   * @param subCollector the collector gathering this searcher's local results
   * @param globalFloor the floor shared by all searchers of this query
   */
  public FloorAwareKnnCollector(AbstractKnnCollector subCollector, GlobalKnnFloor globalFloor) {
    this(subCollector, globalFloor, DEFAULT_GREEDINESS);
  }

  /**
   * Create a collector with an explicit greediness and default slot minimum and sync interval.
   *
   * @param subCollector the collector gathering this searcher's local results
   * @param globalFloor the floor shared by all searchers of this query
   * @param greediness fraction of the search effort that follows the shared floor, in {@code [0,
   *     1]}; see the class comment
   */
  public FloorAwareKnnCollector(
      AbstractKnnCollector subCollector, GlobalKnnFloor globalFloor, float greediness) {
    this(
        subCollector,
        globalFloor,
        greediness,
        DEFAULT_MIN_EXPLORATION_SLOTS,
        DEFAULT_SYNC_INTERVAL);
  }

  /**
   * Create a collector whose ascent gate opens when its local queue fills, with an explicit
   * greediness, slot minimum and sync interval.
   *
   * @param subCollector the collector gathering this searcher's local results
   * @param globalFloor the floor shared by all searchers of this query
   * @param greediness fraction of the search effort that follows the shared floor, in {@code [0,
   *     1]}; see the class comment
   * @param minExplorationSlots smallest permitted size of the greediness clamp's queue, whatever
   *     the greediness; must be at least 1. See {@link #DEFAULT_MIN_EXPLORATION_SLOTS} for the role
   *     this plays in protecting recall.
   * @param syncInterval number of visited vectors between synchronizations with the shared floor;
   *     must be a power of two. Smaller intervals keep the floor fresher (fewer visits) at the
   *     price of more cross-thread traffic.
   */
  public FloorAwareKnnCollector(
      AbstractKnnCollector subCollector,
      GlobalKnnFloor globalFloor,
      float greediness,
      int minExplorationSlots,
      int syncInterval) {
    this(
        subCollector, globalFloor, greediness, minExplorationSlots, syncInterval, subCollector.k());
  }

  /**
   * Create a fully configured collector.
   *
   * @param subCollector the collector gathering this searcher's local results
   * @param globalFloor the floor shared by all searchers of this query
   * @param greediness fraction of the search effort that follows the shared floor, in {@code [0,
   *     1]}; see the class comment
   * @param minExplorationSlots smallest permitted size of the greediness clamp's queue, whatever
   *     the greediness; must be at least 1. See {@link #DEFAULT_MIN_EXPLORATION_SLOTS} for the role
   *     this plays in protecting recall.
   * @param syncInterval number of visited vectors between synchronizations with the shared floor;
   *     must be a power of two. Smaller intervals keep the floor fresher (fewer visits) at the
   *     price of more cross-thread traffic.
   * @param gateK the number of locally collected results after which the shared floor engages, in
   *     {@code [1, subCollector.k()]}. The default, used by the other constructors, is {@code
   *     subCollector.k()}: the floor engages when the local queue fills. Set it below the queue
   *     size when this collector's expected share of the merged top-k is smaller than the queue it
   *     collects into; see the class comment on the ascent gate for when that applies.
   */
  public FloorAwareKnnCollector(
      AbstractKnnCollector subCollector,
      GlobalKnnFloor globalFloor,
      float greediness,
      int minExplorationSlots,
      int syncInterval,
      int gateK) {
    super(subCollector);
    if (greediness < 0 || greediness > 1 || Float.isNaN(greediness)) {
      throw new IllegalArgumentException("greediness must be in [0,1], got: " + greediness);
    }
    if (minExplorationSlots < 1) {
      throw new IllegalArgumentException(
          "minExplorationSlots must be at least 1, got: " + minExplorationSlots);
    }
    if (syncInterval < 1 || Integer.bitCount(syncInterval) != 1) {
      throw new IllegalArgumentException(
          "syncInterval must be a power of two, got: " + syncInterval);
    }
    if (gateK < 1 || gateK > subCollector.k()) {
      throw new IllegalArgumentException(
          "gateK must be in [1, subCollector.k()=" + subCollector.k() + "], got: " + gateK);
    }
    this.subCollector = subCollector;
    this.globalFloor = globalFloor;
    this.gateK = gateK;
    this.syncIntervalMask = syncInterval - 1;
    this.nonCompetitiveQueue =
        new FloatHeap(Math.max(minExplorationSlots, Math.round((1 - greediness) * gateK)));
    this.updatesQueue = new FloatHeap(globalFloor.k());
    this.updatesScratch = new float[globalFloor.k()];
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean localSimUpdated = subCollector.collect(docId, similarity);
    boolean gateJustOpened = gateOpened == false && subCollector.numCollected() >= gateK;
    if (gateJustOpened) {
      gateOpened = true;
    }
    updatesQueue.offer(similarity);
    boolean globalSimUpdated = nonCompetitiveQueue.offer(similarity);

    if (gateOpened && (gateJustOpened || (visitedCount() & syncIntervalMask) == 0)) {
      // The shared heap requires ascending input; draining the pending min-heap yields exactly
      // that. Scores observed before the gate opened are included in the first batch, so nothing
      // seen during the ascent is lost to the shared floor.
      int len = updatesQueue.size();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          updatesScratch[i] = updatesQueue.poll();
        }
        assert updatesQueue.size() == 0;
        cachedGlobalFloor = globalFloor.offer(updatesScratch, len);
        globalSimUpdated = true;
      }
    }
    // Reporting an update whenever the effective bound may have moved (locally or via the shared
    // floor) prompts the graph searcher to re-read minCompetitiveSimilarity() and raise its
    // termination bar promptly.
    return localSimUpdated || globalSimUpdated;
  }

  @Override
  public float minCompetitiveSimilarity() {
    if (gateOpened == false) {
      // Ascent gate: until gateK results have been collected, expose only the delegate's bound,
      // which is NEGATIVE_INFINITY by the KnnCollector contract. See the class comment.
      return subCollector.minCompetitiveSimilarity();
    }
    // nextDown keeps the bound strictly below the floor so exact score ties at the cutoff remain
    // reachable; nextDown of NEGATIVE_INFINITY is NEGATIVE_INFINITY, so an undefined floor is a
    // no-op here.
    return Math.max(
        subCollector.minCompetitiveSimilarity(),
        Math.min(nonCompetitiveQueue.peek(), Math.nextDown(cachedGlobalFloor)));
  }

  @Override
  public String toString() {
    return "FloorAwareKnnCollector[subCollector=" + subCollector + "]";
  }
}
