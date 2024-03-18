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
package org.apache.lucene.backward_index;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestEmptyIndexBackwardsCompatibility extends BackwardsCompatibilityTestBase {
  static final String INDEX_NAME = "empty";
  static final String SUFFIX = "";

  public TestEmptyIndexBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setUseCompoundFile(false)
            .setCodec(TestUtil.getDefaultCodec())
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter writer = new IndexWriter(directory, conf)) {
      writer.flush();
    }
  }

  /** Provides the initial release of the previous major to the test-framework */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() {
    List<Object[]> params = new ArrayList<>();
    // TODO - WHY ONLY on the first major version?
    params.add(new Object[] {Version.LUCENE_9_0_0, createPattern(INDEX_NAME, SUFFIX)});
    return params;
  }

  public void testUpgradeEmptyOldIndex() throws Exception {
    try (Directory dir = newDirectory(directory)) {
      TestIndexUpgradeBackwardsCompatibility.newIndexUpgrader(dir).upgrade();
      TestIndexUpgradeBackwardsCompatibility.checkAllSegmentsUpgraded(dir, 9);
    }
  }
}
