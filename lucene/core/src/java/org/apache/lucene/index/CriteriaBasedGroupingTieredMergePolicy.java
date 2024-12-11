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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decorator Merge policy over the existing Tiered Merge policy. During a segment merge, this policy
 * would categorize segments according to their grouping function outcomes before merging segments
 * within the same category, thus maintaining the grouping criteria’s integrity throughout the merge
 * process.
 *
 * @lucene.experimental
 */
public class CriteriaBasedGroupingTieredMergePolicy extends TieredMergePolicy {

  @Override
  public MergeSpecification findMerges(
      MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
    final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
    MergeSpecification spec = null;
    final Map<String, List<SegmentCommitInfo>> commitInfos = new HashMap<>();
    for (SegmentCommitInfo si : infos) {
      if (merging.contains(si)) {
        continue;
      }

      final String dwptGroupNumber = si.info.getAttribute("dwptGroupNumber");
      commitInfos.computeIfAbsent(dwptGroupNumber, k -> new ArrayList<>()).add(si);
    }

    for (String dwptGroupNumber : commitInfos.keySet()) {
      if (commitInfos.get(dwptGroupNumber).size() > 1) {
        final SegmentInfos newSIS = new SegmentInfos(infos.getIndexCreatedVersionMajor());
        for (SegmentCommitInfo info : commitInfos.get(dwptGroupNumber)) {
          newSIS.add(info);
        }

        final MergeSpecification tieredMergePolicySpec =
            super.findMerges(mergeTrigger, infos, mergeContext);
        if (tieredMergePolicySpec != null) {
          if (spec == null) {
            spec = new MergeSpecification();
          }

          spec.merges.addAll(tieredMergePolicySpec.merges);
        }
      }
    }

    return spec;
  }
}
