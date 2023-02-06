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
package org.apache.lucene.codecs.perfield;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.index.RandomCodec;
import org.apache.lucene.tests.util.TestUtil;
import org.hamcrest.MatcherAssert;

/** Basic tests of PerFieldDocValuesFormat */
public class TestPerFieldKnnVectorsFormat extends BaseKnnVectorsFormatTestCase {
  private Codec codec;

  @Override
  public void setUp() throws Exception {
    codec = new RandomCodec(new Random(random().nextLong()), Collections.emptySet());
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testTwoFieldsTwoFormats() throws IOException {
    try (Directory directory = newDirectory()) {
      // we don't use RandomIndexWriter because it might add more values than we expect !!!!1
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      WriteRecordingKnnVectorsFormat format1 =
          new WriteRecordingKnnVectorsFormat(TestUtil.getDefaultKnnVectorsFormat());
      WriteRecordingKnnVectorsFormat format2 =
          new WriteRecordingKnnVectorsFormat(TestUtil.getDefaultKnnVectorsFormat());
      iwc.setCodec(
          new AssertingCodec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              if ("field1".equals(field)) {
                return format1;
              } else {
                return format2;
              }
            }
          });

      try (IndexWriter iwriter = new IndexWriter(directory, iwc)) {
        Document doc = new Document();
        doc.add(newTextField("id", "1", Field.Store.YES));
        doc.add(new KnnFloatVectorField("field1", new float[] {1, 2, 3}));
        iwriter.addDocument(doc);

        doc.clear();
        doc.add(newTextField("id", "2", Field.Store.YES));
        doc.add(new KnnFloatVectorField("field2", new float[] {4, 5, 6}));
        iwriter.addDocument(doc);
      }

      // Check that each format was used to write the expected field
      MatcherAssert.assertThat(format1.fieldsWritten, equalTo(Set.of("field1")));
      MatcherAssert.assertThat(format2.fieldsWritten, equalTo(Set.of("field2")));

      // Double-check the vectors were written
      try (IndexReader ireader = DirectoryReader.open(directory)) {
        LeafReader reader = ireader.leaves().get(0).reader();
        TopDocs hits1 =
            reader.searchNearestVectors(
                "field1", new float[] {1, 2, 3}, 10, reader.getLiveDocs(), Integer.MAX_VALUE);
        assertEquals(1, hits1.scoreDocs.length);

        TopDocs hits2 =
            reader.searchNearestVectors(
                "field2", new float[] {1, 2, 3}, 10, reader.getLiveDocs(), Integer.MAX_VALUE);
        assertEquals(1, hits2.scoreDocs.length);
      }
    }
  }

  public void testMergeUsesNewFormat() throws IOException {
    try (Directory directory = newDirectory()) {
      IndexWriterConfig initialConfig = newIndexWriterConfig(new MockAnalyzer(random()));
      initialConfig.setMergePolicy(NoMergePolicy.INSTANCE);

      try (IndexWriter iw = new IndexWriter(directory, initialConfig)) {
        for (int i = 0; i < 3; i++) {
          Document doc = new Document();
          doc.add(newTextField("id", "1", Field.Store.YES));
          doc.add(new KnnFloatVectorField("field1", new float[] {1, 2, 3}));
          doc.add(new KnnFloatVectorField("field2", new float[] {1, 2, 3}));
          iw.addDocument(doc);
          iw.commit();
        }
      }

      IndexWriterConfig newConfig = newIndexWriterConfig(new MockAnalyzer(random()));
      WriteRecordingKnnVectorsFormat format1 =
          new WriteRecordingKnnVectorsFormat(TestUtil.getDefaultKnnVectorsFormat());
      WriteRecordingKnnVectorsFormat format2 =
          new WriteRecordingKnnVectorsFormat(TestUtil.getDefaultKnnVectorsFormat());
      newConfig.setCodec(
          new AssertingCodec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
              if ("field1".equals(field)) {
                return format1;
              } else {
                return format2;
              }
            }
          });

      try (IndexWriter iw = new IndexWriter(directory, newConfig)) {
        iw.forceMerge(1);
      }

      // Check that the new format was used while merging
      MatcherAssert.assertThat(format1.fieldsWritten, contains("field1"));
      MatcherAssert.assertThat(format2.fieldsWritten, contains("field2"));
    }
  }

  private static class WriteRecordingKnnVectorsFormat extends KnnVectorsFormat {
    private final KnnVectorsFormat delegate;
    private final Set<String> fieldsWritten;

    public WriteRecordingKnnVectorsFormat(KnnVectorsFormat delegate) {
      super(delegate.getName());
      this.delegate = delegate;
      this.fieldsWritten = new HashSet<>();
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      KnnVectorsWriter writer = delegate.fieldsWriter(state);
      return new KnnVectorsWriter() {

        @Override
        public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
          fieldsWritten.add(fieldInfo.name);
          return writer.addField(fieldInfo);
        }

        @Override
        public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
          writer.flush(maxDoc, sortMap);
        }

        @Override
        public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
          fieldsWritten.add(fieldInfo.name);
          writer.mergeOneField(fieldInfo, mergeState);
        }

        @Override
        public void finish() throws IOException {
          writer.finish();
        }

        @Override
        public void close() throws IOException {
          writer.close();
        }

        @Override
        public long ramBytesUsed() {
          return writer.ramBytesUsed();
        }
      };
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
      return delegate.fieldsReader(state);
    }
  }
}
