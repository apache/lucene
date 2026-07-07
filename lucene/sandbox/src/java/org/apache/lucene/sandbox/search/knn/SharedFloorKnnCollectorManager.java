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

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;

/**
 * A {@link KnnCollectorManager} whose collectors share a {@link GlobalKnnFloor}, so that every
 * segment of a query prunes against a lower bound of the query's final merged top-k cutoff rather
 * than only against its own local top-k.
 *
 * <p>This manager reports {@link #isOptimistic()} and provides {@link #newOptimisticCollector},
 * which means the kNN query machinery runs its regular pro-rata collection strategy on top of it:
 * each segment initially collects only its statistically expected share of the global top-k, and
 * segments whose results prove globally competitive are searched again. The shared floor
 * complements that strategy rather than replacing it. During the first pass it lets segments stop
 * refining results that the merged cutoff has already outrun, and because the floor object is
 * shared across both passes, segments searched again start with the bound the first pass
 * established instead of rediscovering it.
 *
 * <p>The floor may also be fed from outside this process through {@link
 * GlobalKnnFloor#advertise(float)}, allowing several searchers of disjoint indexes, each holding a
 * manager wired to the same query, to bound each other's work. Coordinating who advertises what,
 * and deduplicating documents that may appear in more than one index, is the caller's
 * responsibility; see {@link GlobalKnnFloor} for the exact contract.
 *
 * <p>When the index this manager searches is one shard of a larger corpus, pass the shard's {@code
 * globalShare} (the fraction of the corpus it holds). The manager then opens each collector's
 * ascent gate at the shard's statistically expected contribution to the merged top-k, computed by
 * {@link #perShardGate(int, double)}, instead of at the full local queue size. Without this, a
 * shard's local search cannot know it is one of many — its own index looks like the whole corpus —
 * so it pays the full cost of collecting k results before the shared floor is allowed to end its
 * search, which forfeits most of the cross-shard saving. With the gate at the expected share, a
 * shard pays only the share's fill cost, and then continues past it exactly as long as the shared
 * floor says its results remain globally competitive: hot shards run long, cold shards stop early.
 *
 * <p>Each leaf's scores are published into the shared floor at most once per query. The optimistic
 * strategy searches competitive segments a second time, re-collecting the same documents; if the
 * second pass republished them, the floor's heap would hold duplicate scores for the same
 * documents, and a floor over a multiset with duplicates can exceed the true merged cutoff,
 * breaking the bound's safety (see {@link GlobalKnnFloor}'s distinct-document contract). Collectors
 * created for a leaf that has already been searched therefore read the floor without feeding it.
 *
 * <p>Floor sharing engages only when the query's k reaches an activation threshold; below it this
 * manager creates plain, undecorated collectors and the search is exactly stock search. The two
 * regimes justify the cutoff. The savings available to a shared bound grow with k: the pro-rata
 * strategy sizes each segment's share as {@code k * proportion} plus {@code 16} standard deviations
 * of a binomial, so at small k the statistical padding swamps the share and there is little
 * redundant work left for a floor to eliminate, while at large k the padding is relatively
 * negligible and most per-segment work above the merged cutoff is redundant. The recall risk moves
 * the opposite way: the smaller k is, the closer any valid floor sits to scores an incomplete graph
 * search routes through. Spending nothing and risking nothing below the threshold is therefore the
 * right default, and deployments that have measured recall on their own data can lower it
 * explicitly.
 *
 * <p>A manager, like the floor it holds, carries the state of a single query execution: create one
 * per query, and never share one across queries. Both collector-creation methods may be called
 * concurrently, as segments are searched in parallel.
 *
 * @lucene.experimental
 */
public final class SharedFloorKnnCollectorManager implements KnnCollectorManager {

  /**
   * Default smallest k at which floor sharing engages. See the class comment for why small-k
   * queries are better served by stock search.
   */
  public static final int DEFAULT_FLOOR_ACTIVATION_K = 100;

  /**
   * The number of standard deviations of statistical padding {@link #perShardGate(int, double)}
   * adds to a shard's expected share of the merged top-k; the same value the optimistic
   * multi-segment strategy uses to size per-segment collection ({@code LAMBDA} in {@code
   * AbstractKnnVectorQuery}).
   */
  private static final int LAMBDA = 16;

  private final int k;
  private final GlobalKnnFloor globalFloor;
  private final float greediness;
  private final int floorActivationK;
  private final int minExplorationSlots;
  private final int syncInterval;
  private final float globalShare;

  /**
   * The leaves whose scores have been claimed for publication into the shared floor, so that a
   * second search of the same leaf gets a non-publishing collector; see the class comment. Identity
   * semantics are correct here: a manager lives for one query execution against one reader, and the
   * optimistic strategy passes the same {@link LeafReaderContext} instances to both passes.
   */
  private final Set<LeafReaderContext> publishedLeaves = ConcurrentHashMap.newKeySet();

  /**
   * Create a manager with its own floor and the {@link FloorAwareKnnCollector#DEFAULT_GREEDINESS
   * default greediness}, for queries whose searchers all live in this process.
   *
   * @param k the number of neighbors the query collects
   */
  public SharedFloorKnnCollectorManager(int k) {
    this(k, new GlobalKnnFloor(k), FloorAwareKnnCollector.DEFAULT_GREEDINESS);
  }

  /**
   * Create a manager around an externally provided floor, for queries whose floor is also fed by
   * searchers outside this process.
   *
   * @param k the number of neighbors the query collects
   * @param globalFloor the floor shared by all searchers of this query; its {@link
   *     GlobalKnnFloor#k()} must equal {@code k}, since a floor tracking the wrong result-set size
   *     is not a valid bound
   */
  public SharedFloorKnnCollectorManager(int k, GlobalKnnFloor globalFloor) {
    this(k, globalFloor, FloorAwareKnnCollector.DEFAULT_GREEDINESS);
  }

  /**
   * Create a manager around an externally provided floor with an explicit greediness, applying the
   * {@link #DEFAULT_FLOOR_ACTIVATION_K default activation threshold}.
   *
   * @param k the number of neighbors the query collects
   * @param globalFloor the floor shared by all searchers of this query; its {@link
   *     GlobalKnnFloor#k()} must equal {@code k}
   * @param greediness fraction of each segment's search effort that follows the shared floor, in
   *     {@code [0, 1]}; see {@link FloorAwareKnnCollector}
   */
  public SharedFloorKnnCollectorManager(int k, GlobalKnnFloor globalFloor, float greediness) {
    this(k, globalFloor, greediness, DEFAULT_FLOOR_ACTIVATION_K);
  }

  /**
   * Create a manager around an externally provided floor with an explicit greediness and activation
   * threshold, applying the default slot minimum and sync interval.
   *
   * @param k the number of neighbors the query collects
   * @param globalFloor the floor shared by all searchers of this query; its {@link
   *     GlobalKnnFloor#k()} must equal {@code k}
   * @param greediness fraction of each segment's search effort that follows the shared floor, in
   *     {@code [0, 1]}; see {@link FloorAwareKnnCollector}
   * @param floorActivationK the smallest k at which floor sharing engages; for smaller k this
   *     manager creates plain collectors and the search is exactly stock search. See the class
   *     comment for the reasoning behind the {@link #DEFAULT_FLOOR_ACTIVATION_K default}.
   */
  public SharedFloorKnnCollectorManager(
      int k, GlobalKnnFloor globalFloor, float greediness, int floorActivationK) {
    this(
        k,
        globalFloor,
        greediness,
        floorActivationK,
        FloorAwareKnnCollector.DEFAULT_MIN_EXPLORATION_SLOTS,
        FloorAwareKnnCollector.DEFAULT_SYNC_INTERVAL);
  }

  /**
   * Create a fully configured manager for an index holding the whole corpus. Equivalent to the
   * seven-argument constructor with {@code globalShare = 1}.
   *
   * @param k the number of neighbors the query collects
   * @param globalFloor the floor shared by all searchers of this query; its {@link
   *     GlobalKnnFloor#k()} must equal {@code k}
   * @param greediness fraction of each segment's search effort that follows the shared floor, in
   *     {@code [0, 1]}; see {@link FloorAwareKnnCollector}
   * @param floorActivationK the smallest k at which floor sharing engages; for smaller k this
   *     manager creates plain collectors and the search is exactly stock search. See the class
   *     comment for the reasoning behind the {@link #DEFAULT_FLOOR_ACTIVATION_K default}.
   * @param minExplorationSlots smallest permitted size of each collector's greediness clamp queue;
   *     must be at least 1. See {@link FloorAwareKnnCollector#DEFAULT_MIN_EXPLORATION_SLOTS}.
   * @param syncInterval number of visited vectors between each collector's synchronizations with
   *     the shared floor; must be positive. See {@link
   *     FloorAwareKnnCollector#DEFAULT_SYNC_INTERVAL}.
   */
  public SharedFloorKnnCollectorManager(
      int k,
      GlobalKnnFloor globalFloor,
      float greediness,
      int floorActivationK,
      int minExplorationSlots,
      int syncInterval) {
    this(k, globalFloor, greediness, floorActivationK, minExplorationSlots, syncInterval, 1f);
  }

  /**
   * Create a fully configured manager. Every tuning value the mechanism has is a parameter here;
   * the shorter constructors exist only to supply defaults.
   *
   * @param k the number of neighbors the query collects
   * @param globalFloor the floor shared by all searchers of this query; its {@link
   *     GlobalKnnFloor#k()} must equal {@code k}
   * @param greediness fraction of each segment's search effort that follows the shared floor, in
   *     {@code [0, 1]}; see {@link FloorAwareKnnCollector}. When {@code globalShare} is below 1,
   *     prefer values of {@code 0.9} or above: the greediness clamp is sized from the (now small)
   *     gate, and a low greediness can leave the clamp as the binding constraint for the whole
   *     search, reducing the adaptive floor to a fixed quota. See the caution on {@link
   *     FloorAwareKnnCollector}'s greediness clamp.
   * @param floorActivationK the smallest k at which floor sharing engages; for smaller k this
   *     manager creates plain collectors and the search is exactly stock search. See the class
   *     comment for the reasoning behind the {@link #DEFAULT_FLOOR_ACTIVATION_K default}.
   * @param minExplorationSlots smallest permitted size of each collector's greediness clamp queue;
   *     must be at least 1. See {@link FloorAwareKnnCollector#DEFAULT_MIN_EXPLORATION_SLOTS}.
   * @param syncInterval number of visited vectors between each collector's synchronizations with
   *     the shared floor; must be positive. See {@link
   *     FloorAwareKnnCollector#DEFAULT_SYNC_INTERVAL}.
   * @param globalShare the fraction of the whole corpus held by the index this manager searches, in
   *     {@code (0, 1]}. Pass a value below 1 when this index is one shard of a sharded corpus whose
   *     searchers share the floor across processes; each collector's ascent gate then opens at the
   *     shard's expected contribution to the merged top-k instead of at its full local queue. See
   *     the class comment.
   */
  public SharedFloorKnnCollectorManager(
      int k,
      GlobalKnnFloor globalFloor,
      float greediness,
      int floorActivationK,
      int minExplorationSlots,
      int syncInterval,
      float globalShare) {
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
    Objects.requireNonNull(globalFloor, "globalFloor");
    if (globalFloor.k() != k) {
      throw new IllegalArgumentException(
          "the floor must track the same result-set size as the query: floor k="
              + globalFloor.k()
              + ", query k="
              + k);
    }
    if (greediness < 0 || greediness > 1 || Float.isNaN(greediness)) {
      throw new IllegalArgumentException("greediness must be in [0,1], got: " + greediness);
    }
    if (floorActivationK < 1) {
      throw new IllegalArgumentException(
          "floorActivationK must be at least 1, got: " + floorActivationK);
    }
    if (minExplorationSlots < 1) {
      throw new IllegalArgumentException(
          "minExplorationSlots must be at least 1, got: " + minExplorationSlots);
    }
    if (syncInterval < 1) {
      throw new IllegalArgumentException("syncInterval must be positive, got: " + syncInterval);
    }
    if (globalShare <= 0 || globalShare > 1 || Float.isNaN(globalShare)) {
      throw new IllegalArgumentException("globalShare must be in (0,1], got: " + globalShare);
    }
    this.k = k;
    this.globalFloor = globalFloor;
    this.greediness = greediness;
    this.floorActivationK = floorActivationK;
    this.minExplorationSlots = minExplorationSlots;
    this.syncInterval = syncInterval;
    this.globalShare = globalShare;
  }

  /**
   * The number of results a searcher holding fraction {@code share} of the corpus is expected to
   * contribute to the merged top-k, with the same statistical padding the optimistic multi-segment
   * strategy applies: under random sharding the number of global top-k hits in the shard is
   * binomial, and the gate is sized {@code k * share} plus {@value #LAMBDA} standard deviations,
   * clamped to {@code [1, k]}. This is where a shard's ascent gate should open: below it the shard
   * has not yet collected its statistically due contribution and the shared floor must not end its
   * search; above it, continuing is worthwhile exactly as long as the shard's results remain
   * globally competitive.
   *
   * @param k the number of neighbors the query collects, at least 1
   * @param share the fraction of the corpus the searcher holds, in {@code (0, 1]}
   * @return the gate size, in {@code [1, k]}
   */
  public static int perShardGate(int k, double share) {
    if (k < 1) {
      throw new IllegalArgumentException("k must be at least 1, got: " + k);
    }
    if (share <= 0 || share > 1 || Double.isNaN(share)) {
      throw new IllegalArgumentException("share must be in (0,1], got: " + share);
    }
    // Mirrors perLeafTopKCalculation in AbstractKnnVectorQuery so that a gate computed for a
    // shard equals the quota the optimistic strategy would give a same-sized segment.
    int gate = (int) Math.max(1, k * share + LAMBDA * Math.sqrt(k * share * (1 - share)));
    return Math.min(k, gate);
  }

  /** Return the floor shared by this manager's collectors, so that callers may feed or read it. */
  public GlobalKnnFloor getGlobalFloor() {
    return globalFloor;
  }

  @Override
  public KnnCollector newCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
      throws IOException {
    TopKnnCollector collector = new TopKnnCollector(k, visitedLimit, searchStrategy);
    if (k < floorActivationK) {
      return collector;
    }
    return floorAware(collector, context);
  }

  @Override
  public KnnCollector newOptimisticCollector(
      int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context, int perLeafK)
      throws IOException {
    // The local queue is sized to the segment's pro-rata share; the floor itself always tracks
    // the full k best across the query. Activation is decided by the query's k, not the segment's
    // share: the policy is about the query.
    TopKnnCollector collector = new TopKnnCollector(perLeafK, visitedLimit, searchStrategy);
    if (k < floorActivationK) {
      return collector;
    }
    return floorAware(collector, context);
  }

  private FloorAwareKnnCollector floorAware(TopKnnCollector collector, LeafReaderContext context) {
    // Publish each leaf's scores at most once per query; a second search of the same leaf
    // re-collects the same documents, and republishing them would violate the floor's
    // distinct-document contract. See the class comment.
    boolean publish = context == null || publishedLeaves.add(context);
    return new FloorAwareKnnCollector(
        collector,
        globalFloor,
        greediness,
        minExplorationSlots,
        syncInterval,
        gateFor(context, collector.k()),
        publish);
  }

  /**
   * The ascent gate for a collector over {@code context}: the leaf's statistically expected
   * contribution to the merged top-k, never more than the local queue it collects into. The leaf's
   * share of the whole corpus is its share of this index scaled by the index's {@code globalShare}.
   * When that combined share is 1 — a single-segment index holding the entire corpus — the gate is
   * the queue size and behavior is exactly the default: the local queue is the query's own top-k,
   * and only external advertisements could justify ending the search below it.
   */
  private int gateFor(LeafReaderContext context, int queueSize) {
    double leafGlobalShare = globalShare;
    if (context != null && context.parent != null) {
      leafGlobalShare *= context.reader().maxDoc() / (double) context.parent.reader().maxDoc();
    }
    if (leafGlobalShare <= 0 || leafGlobalShare >= 1) {
      return queueSize;
    }
    return Math.min(queueSize, perShardGate(k, leafGlobalShare));
  }

  @Override
  public boolean isOptimistic() {
    return true;
  }
}
