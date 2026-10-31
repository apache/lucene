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

/**
 * Shared-floor kNN collection: collectors that prune each searcher's graph exploration against a
 * lower bound of the query's final merged top-k cutoff, shared across index segments and, through
 * {@link org.apache.lucene.sandbox.search.knn.GlobalKnnFloor#advertise(float)}, across searchers in
 * other processes.
 *
 * <h2>Use case and acceptance criterion</h2>
 *
 * <p>The intended use case is large-k retrieval across disjoint shards. Each shard would otherwise
 * search for its own k results even though only k results survive the global merge. A shared bound
 * can reduce exploration while retaining each shard's ability to contribute more than a fixed
 * quota. The number of discarded results is not an estimate of avoidable graph traversal work.
 *
 * <p>The performance objective is less work and lower latency at recall comparable to stock Lucene,
 * not exact nearest-neighbor retrieval: the baseline is already approximate. Gate size, exploration
 * width, activation threshold, and synchronization interval must be calibrated against that
 * baseline. Aggressive settings in a parameter sweep can lose recall; a deployment should use only
 * operating points that meet its recall criterion. Small-k queries should bypass sharing unless
 * measurements justify enabling it. The default activation threshold is a policy default, not a
 * claim that every query above it benefits.
 *
 * <h2>Bound validity and graph traversal</h2>
 *
 * <p>The k-th best score among any subset of distinct matching documents is a lower bound on the
 * exact global k-th best score. The floor can therefore exclude a document's score from the global
 * top-k, but cannot establish that the document is unnecessary as a bridge in an HNSW graph. The
 * ascent gate and score clamp are exploration heuristics, not proofs of recall parity. Zero
 * greediness disables floor pruning even when the ascent gate is smaller than the local queue; the
 * collector may still publish scores to help other participants.
 *
 * <h2>Execution and transport contract</h2>
 *
 * <p>Create fresh managers and floors for each query execution. All participants must use the same
 * query, global k, eligibility rules, and comparable similarity scores. Publish each document's
 * score at most once across participants; replicas and retries require caller-side deduplication.
 * An advertised scalar must itself be a valid bound based on at least k distinct matching hits, not
 * a partial shard's best score or its smaller local quota's cutoff. Scalar bound updates may be
 * repeated or reordered because they are combined by maximum; raw score batches may not be
 * duplicated. Transport, query identity, cancellation, and replica handling belong to the caller.
 *
 * <h2>Evaluating a configuration</h2>
 *
 * <p>Compare independent and shared-floor searches on identical shard graphs and queries, with
 * exact global ground truth for recall. Sweep the independent and shared configurations over an
 * overlapping recall range and retain the settings that satisfy the chosen recall tolerance. Report
 * per-query recall differences, total vector comparisons, and end-to-end latency separately. A
 * reduction in comparisons does not by itself measure a latency speedup. Include synchronization
 * and transport costs in latency measurements, and measure filtered queries separately because
 * core's exact-search fallback can erase traversal savings.
 *
 * <p>Record source revisions, parameters, shard layout, query set, and repeated runs. For
 * concurrent searches, floor timing can change both visits and results. Test graph-traversal
 * behavior on segments large enough to avoid the codec's exhaustive-scan heuristic; exact scans
 * remain useful for validating score bounds but do not validate HNSW exploration.
 */
package org.apache.lucene.sandbox.search.knn;
