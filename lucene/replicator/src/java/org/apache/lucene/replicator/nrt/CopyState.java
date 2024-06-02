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

package org.apache.lucene.replicator.nrt;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.SegmentInfos;

/**
 * Holds incRef'd file level details for one point-in-time segment infos on the primary node.
 *
 * @param infos only non-null on the primary node
 * @lucene.experimental
 */
public record CopyState(
    Map<String, FileMetaData> files,
    long version,
    long gen,
    byte[] infosBytes,
    Set<String> completedMergeFiles,
    long primaryGen,
    SegmentInfos infos) {

  public CopyState {
    assert completedMergeFiles != null;
    files = Collections.unmodifiableMap(files);
    completedMergeFiles = Collections.unmodifiableSet(completedMergeFiles);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(version=" + version + ")";
  }
}
