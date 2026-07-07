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
 *       <p><b>Caution:</b> because the clamp is sized from {@code gateK}, a low greediness combined
 *       with a gate well below the local queue size can leave the clamp as the binding constraint
 *       for the entire search, silently reducing the collector to a fixed per-searcher quota of
 *       {@code (1 - greediness) * gateK} results — the floor's adaptivity is neutralized, and
 *       measurements of such a configuration measure the quota, not the floor. (Benchmarked
 *       directly: {@code greediness = 0.5, gateK = 6000} on a 16-shard k=10000 search reproduced a
 *       static per-shard quota of 3000 result-for-result.) When configuring a gate below the queue
 *       size, derive greediness from the exploration width the graph needs via {@link
 *       #greedinessForClamp(int, int)} instead of choosing a constant fraction.
 *   <li><b>Batched synchronization.</b> Scores are published to the shared floor, and the floor is
 *       re-read, when the ascent gate opens and each time {@code syncInterval} further vectors have
 *       been visited (default {@value #DEFAULT_SYNC_INTERVAL}), so the shared state is touched a
 *       constant number of times per few hundred scored candidates rather than once per candidate.
 *       A stale floor can only delay termination, never cause a wrong result, so the interval
 *       trades a bounded amount of extra work for the absence of cross-thread traffic in the
 *       scoring loop.
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

  /**
   * The greediness at which the clamp keeps {@code clampSlots} exploration slots against a gate of
   * {@code gateK}: {@code 1 - clampSlots / gateK}, or 0 when the requested width is the whole gate
   * or more (the floor is then fully neutralized).
   *
   * <p>Prefer deriving greediness through this method over choosing a constant. What protects
   * recall is the clamp's <em>absolute</em> width, not the greediness fraction: the same {@code
   * greediness = 0.9} that leaves a comfortable 600-slot clamp at {@code gateK = 6000} collapses to
   * the bare minimum at {@code gateK = 184}, which benchmarked at double-digit recall loss. A
   * caller who instead fixes the width it needs, and lets this method translate it, is safe at
   * every gate size.
   *
   * @param gateK the ascent gate the collector will use, at least 1
   * @param clampSlots the exploration width to preserve, at least 1
   */
  public static float greedinessForClamp(int gateK, int clampSlots) {
    if (gateK < 1) {
      throw new IllegalArgumentException("gateK must be at least 1, got: " + gateK);
    }
    if (clampSlots < 1) {
      throw new IllegalArgumentException("clampSlots must be at least 1, got: " + clampSlots);
    }
    if (clampSlots >= gateK) {
      return 0f;
    }
    return 1f - clampSlots / (float) gateK;
  }

  private final AbstractKnnCollector subCollector;
  private final GlobalKnnFloor globalFloor;

  /**
   * Number of locally collected results after which the shared floor engages; the ascent gate's
   * threshold. See the class comment.
   */
  private final int gateK;

  /** Number of visited vectors between synchronizations with the shared floor. */
  private final int syncInterval;

  /**
   * Whether this collector publishes its observed similarities into the shared floor. A
   * non-publishing collector still reads the floor and prunes against it; it only never feeds it.
   * Used when the same documents' scores have already been published by an earlier search of the
   * same leaf (the optimistic strategy's second pass), where publishing again would insert
   * duplicate scores into the floor's heap and could inflate the floor above the true merged
   * cutoff, violating {@link GlobalKnnFloor}'s distinct-document contract.
   */
  private final boolean publishToFloor;

  /**
   * The best {@code max(minExplorationSlots, (1 - greediness) * k)} similarities seen by this
   * collector, competitive or not. Its minimum caps the effective bound, implementing the
   * greediness clamp.
   */
  private final FloatHeap nonCompetitiveQueue;

  /**
   * Similarities observed since the last synchronization, awaiting publication; {@code null} for a
   * non-publishing collector.
   */
  private final FloatHeap updatesQueue;

  /**
   * Scratch used to drain {@link #updatesQueue} in ascending order for batch publication; {@code
   * null} for a non-publishing collector.
   */
  private final float[] updatesScratch;

  private boolean gateOpened;

  /**
   * The visited count at or beyond which the next synchronization with the shared floor happens.
   * Deliberately a threshold rather than an exact boundary: {@link #collect} is invoked only for
   * candidates that beat the current bound, so visited counts are consulted at irregular strides
   * and an equality test would skip every boundary that falls between two collects — precisely in
   * the late phase of the search, when few candidates beat the bar and the floor matters most.
   */
  private long nextSyncAt;

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
   *     must be positive. Smaller intervals keep the floor fresher (fewer visits) at the price of
   *     more cross-thread traffic.
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
   *     must be positive. Smaller intervals keep the floor fresher (fewer visits) at the price of
   *     more cross-thread traffic.
   * @param gateK the number of locally collected results after which the shared floor engages, in
   *     {@code [1, subCollector.k()]}. The default, used by the other constructors, is {@code
   *     subCollector.k()}: the floor engages when the local queue fills. Set it below the queue
   *     size when this collector's expected share of the merged top-k is smaller than the queue it
   *     collects into; see the class comment on the ascent gate for when that applies, and the
   *     greediness clamp caution for the interaction between a below-queue gate and low greediness.
   */
  public FloorAwareKnnCollector(
      AbstractKnnCollector subCollector,
      GlobalKnnFloor globalFloor,
      float greediness,
      int minExplorationSlots,
      int syncInterval,
      int gateK) {
    this(subCollector, globalFloor, greediness, minExplorationSlots, syncInterval, gateK, true);
  }

  /**
   * Create a fully configured collector that may be barred from publishing. Package-private: used
   * by {@link SharedFloorKnnCollectorManager} to enforce {@link GlobalKnnFloor}'s distinct-document
   * contract by publishing each leaf's scores at most once per query; see {@link #publishToFloor}.
   */
  FloorAwareKnnCollector(
      AbstractKnnCollector subCollector,
      GlobalKnnFloor globalFloor,
      float greediness,
      int minExplorationSlots,
      int syncInterval,
      int gateK,
      boolean publishToFloor) {
    super(subCollector);
    if (greediness < 0 || greediness > 1 || Float.isNaN(greediness)) {
      throw new IllegalArgumentException("greediness must be in [0,1], got: " + greediness);
    }
    if (minExplorationSlots < 1) {
      throw new IllegalArgumentException(
          "minExplorationSlots must be at least 1, got: " + minExplorationSlots);
    }
    if (syncInterval < 1) {
      throw new IllegalArgumentException("syncInterval must be positive, got: " + syncInterval);
    }
    if (gateK < 1 || gateK > subCollector.k()) {
      throw new IllegalArgumentException(
          "gateK must be in [1, subCollector.k()=" + subCollector.k() + "], got: " + gateK);
    }
    this.subCollector = subCollector;
    this.globalFloor = globalFloor;
    this.gateK = gateK;
    this.syncInterval = syncInterval;
    this.publishToFloor = publishToFloor;
    this.nonCompetitiveQueue =
        new FloatHeap(Math.max(minExplorationSlots, Math.round((1 - greediness) * gateK)));
    this.updatesQueue = publishToFloor ? new FloatHeap(globalFloor.k()) : null;
    this.updatesScratch = publishToFloor ? new float[globalFloor.k()] : null;
  }

  /** The ascent gate's threshold; package-private, for tests and the manager. */
  int gateK() {
    return gateK;
  }

  /** Whether this collector feeds the shared floor; package-private, for tests. */
  boolean publishesToFloor() {
    return publishToFloor;
  }

  @Override
  public boolean collect(int docId, float similarity) {
    boolean localSimUpdated = subCollector.collect(docId, similarity);
    boolean gateJustOpened = gateOpened == false && subCollector.numCollected() >= gateK;
    if (gateJustOpened) {
      gateOpened = true;
    }
    if (publishToFloor) {
      updatesQueue.offer(similarity);
    }
    boolean globalSimUpdated = nonCompetitiveQueue.offer(similarity);

    if (gateOpened && (gateJustOpened || visitedCount() >= nextSyncAt)) {
      nextSyncAt = visitedCount() + syncInterval;
      if (publishToFloor) {
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
      } else {
        float refreshed = globalFloor.floor();
        if (refreshed > cachedGlobalFloor) {
          cachedGlobalFloor = refreshed;
          globalSimUpdated = true;
        }
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
