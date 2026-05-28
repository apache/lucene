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
 * De-duplicating flat vector format and matching HNSW wrapper. Storage co-locates unique vectors
 * across multiple fields with matching {@code (dimension, encoding)} into a per-segment "pool",
 * dramatically reducing index size when the same vector is referenced by many docs/fields (e.g.
 * separate per-filter HNSW graphs that share underlying vectors).
 *
 * <p>See {@link org.apache.lucene.codecs.dedup.DedupFlatVectorsFormat} for the on-disk layout and
 * {@link org.apache.lucene.codecs.dedup.DedupHnswVectorsFormat} for the HNSW wrapper.
 *
 * @lucene.experimental
 */
package org.apache.lucene.codecs.dedup;
