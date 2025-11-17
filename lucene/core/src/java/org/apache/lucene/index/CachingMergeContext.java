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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.lucene.util.InfoStream;

/**
 * a wrapper of IndexWriter MergeContext. Try to cache the {@link
 * #numDeletesToMerge(SegmentCommitInfo)} result in merge phase, to avoid duplicate calculation
 */
class CachingMergeContext implements MergePolicy.MergeContext {
  final MergePolicy.MergeContext mergeContext;
  final HashMap<SegmentCommitInfo, Integer> cachedNumDeletesToMerge = new HashMap<>();

  CachingMergeContext(MergePolicy.MergeContext mergeContext) {
    this.mergeContext = mergeContext;
  }

  @Override
  public final int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
    Integer numDeletesToMerge = cachedNumDeletesToMerge.get(info);
    if (numDeletesToMerge != null) {
      return numDeletesToMerge;
    }
    numDeletesToMerge = mergeContext.numDeletesToMerge(info);
    cachedNumDeletesToMerge.put(info, numDeletesToMerge);
    return numDeletesToMerge;
  }

  @Override
  public final int numDeletedDocs(SegmentCommitInfo info) {
    return mergeContext.numDeletedDocs(info);
  }

  @Override
  public final InfoStream getInfoStream() {
    return mergeContext.getInfoStream();
  }

  @Override
  public final Set<SegmentCommitInfo> getMergingSegments() {
    return mergeContext.getMergingSegments();
  }
}
