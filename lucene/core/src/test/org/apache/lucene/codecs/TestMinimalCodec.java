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
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests to ensure that {@link Codec}s won't need to implement all formats in case where only a
 * small subset of Lucene's functionality is used.
 */
public class TestMinimalCodec extends LuceneTestCase {

  public void testMinimalCodec() throws IOException {
    runMinimalCodecTest(false, false);
    runMinimalCodecTest(false, true);
    runMinimalCodecTest(true, true);
    runMinimalCodecTest(true, false);
  }

  private void runMinimalCodecTest(boolean useCompoundFile, boolean useDeletes) throws IOException {
    try (BaseDirectoryWrapper dir = newDirectory()) {
      dir.setCheckIndexOnClose(false); // MinimalCodec is not registered with SPI

      IndexWriterConfig writerConfig =
          newIndexWriterConfig(new MockAnalyzer(random()))
              .setCodec(new MinimalCodec(useCompoundFile, useDeletes))
              .setUseCompoundFile(useCompoundFile);
      if (!useCompoundFile) {
        writerConfig.getMergePolicy().setNoCFSRatio(0.0);
        writerConfig.getMergePolicy().setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
      }

      int expectedNumDocs = 0;
      try (IndexWriter writer = new IndexWriter(dir, writerConfig)) {
        writer.addDocument(basicDocument());
        expectedNumDocs += 1;
        writer.flush(); // create second segment
        if (useDeletes && randomBoolean()) {
          writer.deleteDocuments(new MatchAllDocsQuery());
          expectedNumDocs = 0;
        }
        writer.addDocument(basicDocument());
        expectedNumDocs += 1;
        writer.forceMerge(1); // test merges
        if (useDeletes && randomBoolean()) {
          writer.deleteDocuments(new MatchAllDocsQuery());
          expectedNumDocs = 0;
        }
        if (randomBoolean()) {
          writer.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(expectedNumDocs, reader.numDocs());
        }
      }
    }
  }

  /** returns a basic document with no indexed fields */
  private static Document basicDocument() {
    Document doc = new Document();
    if (randomBoolean()) {
      doc.add(new StoredField("field", "value"));
    }
    return doc;
  }

  /** Minimal codec implementation for working with the most basic documents */
  private static class MinimalCodec extends Codec {

    private final Codec wrappedCodec = TestUtil.getDefaultCodec();
    private final boolean useCompoundFormat;
    private final boolean useDeletes;

    protected MinimalCodec(boolean useCompoundFormat, boolean useDeletes) {
      super("MinimalCodec");
      this.useCompoundFormat = useCompoundFormat;
      this.useDeletes = useDeletes;
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
      if (useCompoundFormat) {
        return wrappedCodec.compoundFormat();
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
      if (useDeletes) {
        return wrappedCodec.liveDocsFormat();
      } else {
        throw new UnsupportedOperationException();
      }
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
}
