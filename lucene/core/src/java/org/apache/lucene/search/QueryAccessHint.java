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
package org.apache.lucene.search;

/**
 * Hint describing the access pattern of a query, intended to be consulted by readers when wiring up
 * per-query iteration.
 *
 * <p>This intentionally mirrors {@link org.apache.lucene.store.DataAccessHint} in spirit: it
 * describes <em>what is likely to happen</em>, not what mechanism should be used to satisfy it.
 * Readers may consult this hint to choose between, e.g., aggressive prefetch vs. cache-friendly
 * traversal, but they are not required to.
 *
 * @lucene.experimental
 */
public enum QueryAccessHint implements QueryReadHint {
  /**
   * The query expects to perform a small number of accesses and is latency-sensitive. Readers may
   * favor strategies that reduce per-access latency, such as prefetching the blocks a single
   * traversal is about to touch.
   *
   * <p>This is advisory and best-effort: a reader with no cheaper-latency strategy simply ignores
   * it. Among the built-in formats it is consumed by {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader} on the HNSW graph-search path for
   * full-precision float32, byte, and scalar-quantised vectors (each batch of neighbour vectors is
   * prefetched before scoring). Other vector encodings — e.g. bit vectors, or off-heap values whose
   * representation does not override {@code prefetch} — currently ignore it, so setting POINT over
   * those fields is a silent no-op rather than an error.
   */
  POINT
}
