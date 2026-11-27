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

package org.apache.lucene.sandbox.codecs.segmentivf;

/**
 * How a filtered search applies its filter.
 *
 * <p>A {@link Mode} is chosen per query through {@link SegmentIVFVectorsFormat.SearchStrategy}.
 * Both a plain {@code KnnFloatVectorQuery} and {@link SegmentIVFKnnQuery} hand the codec the
 * filter's matches as a bit set, through {@code AcceptDocs}, so both push it down.
 *
 * <p>Whatever the mode, a filter small enough to rerank whole is searched exactly, every match
 * reranked.
 *
 * @lucene.experimental
 */
public final class FilterPlan {
  /** How a filtered search applies its filter. */
  public enum Mode {
    /**
     * Collect the filter's matches into a bit set per segment, through {@code AcceptDocs}, and test
     * each probed slot against it.
     */
    PUSHDOWN
  }

  private FilterPlan() {}
}
