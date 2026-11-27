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
 * nearest-neighbor search: 1-bit coarse codes are scanned, and INT8 fine codes rerank. {@link
 * org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat} describes the design and the
 * files; {@link org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFKnnQuery} searches a whole
 * index with one rerank. The format indexes FLOAT32 vectors with COSINE or DOT_PRODUCT similarity.
 *
 * <p>The io_uring and mapped-view code uses restricted foreign-function calls, so run with {@code
 * --enable-native-access}. The scan's SIMD kernels use the Vector API when {@code --add-modules
 * jdk.incubator.vector} is given, and fall back to scalar code otherwise.
 *
 * @lucene.experimental
 */
package org.apache.lucene.sandbox.codecs.segmentivf;
