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
import java.util.List;

/**
 * Selects merge groups for {@link ProactiveMergePolicy} to run around a wrapped policy.
 *
 * <p>Returned groups keep segment order. Wrapped log policies expect adjacent groups; a group that
 * skips a segment in between can change document order. A null group, an empty group, a null
 * member, or a repeated segment fails an assertion and is skipped. A segment that is already
 * merging, or that is not in {@code infos}, drops the whole group so the other members stay
 * available to the wrapped policy.
 *
 * @lucene.experimental
 */
@FunctionalInterface
public interface MergeSelector {

  /** Returns merge groups in segment order. An empty list takes nothing. */
  List<List<SegmentCommitInfo>> select(
      MergeTrigger trigger, SegmentInfos infos, MergePolicy.MergeContext context)
      throws IOException;
}
