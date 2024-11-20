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
package org.apache.lucene.search.uhighlight;

import com.carrotsearch.randomizedtesting.annotations.Name;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;
import org.junit.After;
import org.junit.Before;

/** Parent class for unified highlighter tests. */
abstract class UnifiedHighlighterTestBase extends LuceneTestCase {
  /** Randomized field type for the "body" field, generally. */
  protected final FieldType fieldType;

  protected MockAnalyzer indexAnalyzer;
  protected Directory dir;

  static final FieldType postingsType = new FieldType(TextField.TYPE_STORED);
  static final FieldType tvType = new FieldType(TextField.TYPE_STORED);
  static final FieldType postingsWithTvType = new FieldType(TextField.TYPE_STORED);
  static final FieldType reanalysisType = TextField.TYPE_STORED;

  static {
    postingsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    postingsType.freeze();

    tvType.setStoreTermVectors(true);
    tvType.setStoreTermVectorPositions(true);
    tvType.setStoreTermVectorOffsets(true);
    tvType.freeze();

    postingsWithTvType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    postingsWithTvType.setStoreTermVectors(true);
    postingsWithTvType.freeze();

    // re-analysis type needs no further changes.
  }

  UnifiedHighlighterTestBase(@Name("fieldType") FieldType fieldType) {
    this.fieldType = fieldType;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(dir);
    super.tearDown();
  }

  /**
   * @return Returns a {@link RandomIndexWriter} but avoids using random merge policy, which may
   *     reorder documents (which assertions rely on).
   */
  static RandomIndexWriter newIndexOrderPreservingWriter(Directory dir, Analyzer indexAnalyzer)
      throws IOException {
    return new RandomIndexWriter(
        random(),
        dir,
        LuceneTestCase.newIndexWriterConfig(indexAnalyzer)
            .setMergePolicy(LuceneTestCase.newMergePolicy(random(), false)));
  }

  /**
   * @return Returns a {@link RandomIndexWriter} but avoids using random merge policy, which may
   *     reorder documents (which assertions rely on).
   */
  RandomIndexWriter newIndexOrderPreservingWriter() throws IOException {
    return newIndexOrderPreservingWriter(dir, indexAnalyzer);
  }

  public static FieldType randomFieldType(Random random, FieldType... typePossibilities) {
    if (typePossibilities == null || typePossibilities.length == 0) {
      typePossibilities =
          new FieldType[] {postingsType, tvType, postingsWithTvType, reanalysisType};
    }
    return typePossibilities[random.nextInt(typePossibilities.length)];
  }

  /** for {@link com.carrotsearch.randomizedtesting.annotations.ParametersFactory} */
  // https://github.com/carrotsearch/randomizedtesting/blob/master/examples/maven/src/main/java/com/carrotsearch/examples/randomizedrunner/Test007ParameterizedTests.java
  static Iterable<Object[]> parametersFactoryList() {
    return Arrays.asList(
        new Object[][] {{postingsType}, {tvType}, {postingsWithTvType}, {reanalysisType}});
  }
}
