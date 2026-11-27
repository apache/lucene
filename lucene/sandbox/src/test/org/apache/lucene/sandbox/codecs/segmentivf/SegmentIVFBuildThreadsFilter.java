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

import com.carrotsearch.randomizedtesting.ThreadFilter;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;

/**
 * Ignores SegmentIVF's process-lifetime daemon threads when checking for leaked threads: the shared
 * build pool of {@code Parallel}, which outlives whichever suite first starts it.
 *
 * <p>Also applies the filter {@code LuceneTestCase} declares, which a suite's own {@code
 * ThreadLeakFilters} replaces.
 */
public final class SegmentIVFBuildThreadsFilter implements ThreadFilter {
  private final ThreadFilter quickPatch = new QuickPatchThreadsFilter();

  @Override
  public boolean reject(Thread t) {
    final String name = t.getName();
    if (name != null && name.startsWith("segmentivf-build-")) {
      return true;
    }
    return quickPatch.reject(t);
  }
}
