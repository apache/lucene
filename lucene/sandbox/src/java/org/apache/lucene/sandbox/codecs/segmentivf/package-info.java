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
 * SegmentIVF, a two-tier inverted-file (IVF) vector format for low-latency approximate
 * nearest-neighbor search.
 *
 * <p>Vectors are normalized, rotated with a randomized Hadamard transform and clustered into {@code
 * nlist} cells per segment, with up to {@code spillBits} extra copies near cell boundaries. Each
 * copy stores a 2-bit Nitrox2 coarse code (compared by XOR and popcount) and an INT8 or FP32 fine
 * record. Flushes and merges warm-start clustering from segments this process already wrote.
 *
 * <p>A query picks cells through a graph over the centroid codes, scans their coarse codes with
 * SIMD Hamming kernels, and keeps a deduplicated shortlist per segment. {@link
 * org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFKnnQuery} merges those shortlists and
 * fine-reranks only the index-wide best, so fine reads do not grow with the segment count; a plain
 * {@code KnnFloatVectorQuery} reranks per segment. Filters apply before any scoring.
 *
 * <p>Coarse codes are copied off-heap on a background thread, within a budget derived from the
 * cgroup memory limit and the heap, so the page cache cannot evict them. Fine records are read
 * mapped while they fit in memory, and with one batched io_uring submission per rerank when they do
 * not or the cgroup reports memory pressure. The io_uring and mapped-view code uses restricted
 * foreign-function calls, so run with {@code --enable-native-access}.
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.codecs.segmentivf;
