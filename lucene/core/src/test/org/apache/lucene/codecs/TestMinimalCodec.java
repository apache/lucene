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
package org.apache.lucene.codecs;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests to ensure that {@link Codec}s won't need to implement all formats in case where only a
 * small subset of Lucene's functionality is used.
 */
public class TestMinimalCodec extends LuceneTestCase {

  public void testMinimalCodec() throws IOException {
    runMinimalCodecTest(false);
  }

  public void testMinimalCompoundCodec() throws IOException {
    runMinimalCodecTest(true);
  }

  private void runMinimalCodecTest(boolean useCompoundFile) throws IOException {
    try (BaseDirectoryWrapper dir = newDirectory()) {
      IndexWriterConfig writerConfig =
          newIndexWriterConfig(new MockAnalyzer(random()))
              .setCodec(useCompoundFile ? new MinimalCompoundCodec() : new MinimalCodec())
              .setUseCompoundFile(useCompoundFile);
      if (!useCompoundFile) {
        // Avoid using MockMP as it randomly enables compound file creation
        writerConfig.setMergePolicy(newMergePolicy(random(), false));
        writerConfig.getMergePolicy().setNoCFSRatio(0.0);
        writerConfig.getMergePolicy().setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
      }

      try (IndexWriter writer = new IndexWriter(dir, writerConfig)) {
        writer.addDocument(basicDocument());
        writer.flush();
        // create second segment
        writer.addDocument(basicDocument());
        writer.forceMerge(1); // test merges
        if (randomBoolean()) {
          writer.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(2, reader.numDocs());
        }
      }
    }
  }

  /** returns a basic document with no indexed fields */
  private static Document basicDocument() {
    return new Document();
  }

  /** Minimal codec implementation for working with the most basic documents */
  public static class MinimalCodec extends Codec {

    protected final Codec wrappedCodec = TestUtil.getDefaultCodec();

    public MinimalCodec() {
      this("MinimalCodec");
    }

    protected MinimalCodec(String name) {
      super(name);
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
      return wrappedCodec.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
      return wrappedCodec.segmentInfoFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
      // TODO: avoid calling this when no stored fields are written or read
      return wrappedCodec.storedFieldsFormat();
    }

    @Override
    public PostingsFormat postingsFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public NormsFormat normsFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PointsFormat pointsFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Minimal codec implementation for working with the most basic documents, supporting compound
   * formats
   */
  public static class MinimalCompoundCodec extends MinimalCodec {
    public MinimalCompoundCodec() {
      super("MinimalCompoundCodec");
    }

    @Override
    public CompoundFormat compoundFormat() {
      return wrappedCodec.compoundFormat();
    }
  }
}
