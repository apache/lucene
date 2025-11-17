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
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.InfoStream;

public class TestCachingMergeContext extends LuceneTestCase {
  public void testNumDeletesToMerge() throws IOException {
    MockMergeContext mergeContext = new MockMergeContext();
    CachingMergeContext cachingMergeContext = new CachingMergeContext(mergeContext);
    assertEquals(cachingMergeContext.numDeletesToMerge(null), 1);
    assertEquals(cachingMergeContext.cachedNumDeletesToMerge.size(), 1);
    assertEquals(
        cachingMergeContext.cachedNumDeletesToMerge.getOrDefault(null, -1), Integer.valueOf(1));
    assertEquals(mergeContext.count, 1);

    // increase the mock count
    mergeContext.numDeletesToMerge(null);
    assertEquals(mergeContext.count, 2);

    // assert the cache result still one
    assertEquals(cachingMergeContext.numDeletesToMerge(null), 1);
    assertEquals(cachingMergeContext.cachedNumDeletesToMerge.size(), 1);
    assertEquals(
        cachingMergeContext.cachedNumDeletesToMerge.getOrDefault(null, -1), Integer.valueOf(1));
  }

  private static final class MockMergeContext implements MergePolicy.MergeContext {
    int count = 0;

    @Override
    public final int numDeletesToMerge(SegmentCommitInfo info) throws IOException {
      this.count += 1;
      return this.count;
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo info) {
      return 0;
    }

    @Override
    public InfoStream getInfoStream() {
      return null;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
      return null;
    }
  }
}
