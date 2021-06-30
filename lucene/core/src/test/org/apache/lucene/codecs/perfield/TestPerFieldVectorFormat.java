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

import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.codecs.VectorReader;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.BaseVectorFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.hamcrest.MatcherAssert;

/** Basic tests of PerFieldDocValuesFormat */
public class TestPerFieldVectorFormat extends BaseVectorFormatTestCase {
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
      WriteRecordingVectorFormat format1 =
          new WriteRecordingVectorFormat(TestUtil.getDefaultVectorFormat());
      WriteRecordingVectorFormat format2 =
          new WriteRecordingVectorFormat(TestUtil.getDefaultVectorFormat());
      iwc.setCodec(
          new AssertingCodec() {
            @Override
            public VectorFormat getVectorFormatForField(String field) {
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
        doc.add(new VectorField("field1", new float[] {1, 2, 3}));
        iwriter.addDocument(doc);

        doc.clear();
        doc.add(newTextField("id", "2", Field.Store.YES));
        doc.add(new VectorField("field2", new float[] {4, 5, 6}));
        iwriter.addDocument(doc);
      }

      // Check that each format was used to write the expected field
      MatcherAssert.assertThat(format1.fieldsWritten, equalTo(Set.of("field1")));
      MatcherAssert.assertThat(format2.fieldsWritten, equalTo(Set.of("field2")));

      // Double-check the vectors were written
      try (IndexReader ireader = DirectoryReader.open(directory)) {
        TopDocs hits1 =
            ireader
                .leaves()
                .get(0)
                .reader()
                .searchNearestVectors("field1", new float[] {1, 2, 3}, 10, 1);
        assertEquals(1, hits1.scoreDocs.length);
        TopDocs hits2 =
            ireader
                .leaves()
                .get(0)
                .reader()
                .searchNearestVectors("field2", new float[] {1, 2, 3}, 10, 1);
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
          doc.add(new VectorField("field", new float[] {1, 2, 3}));
          iw.addDocument(doc);
          iw.commit();
        }
      }

      IndexWriterConfig newConfig = newIndexWriterConfig(new MockAnalyzer(random()));
      WriteRecordingVectorFormat newFormat =
          new WriteRecordingVectorFormat(TestUtil.getDefaultVectorFormat());
      newConfig.setCodec(
          new AssertingCodec() {
            @Override
            public VectorFormat getVectorFormatForField(String field) {
              return newFormat;
            }
          });

      try (IndexWriter iw = new IndexWriter(directory, newConfig)) {
        iw.forceMerge(1);
      }

      // Check that the new format was used while merging
      MatcherAssert.assertThat(newFormat.fieldsWritten, equalTo(Set.of("field")));
    }
  }

  private static class WriteRecordingVectorFormat extends VectorFormat {
    private final VectorFormat delegate;
    private final Set<String> fieldsWritten;

    public WriteRecordingVectorFormat(VectorFormat delegate) {
      super(delegate.getName());
      this.delegate = delegate;
      this.fieldsWritten = new HashSet<>();
    }

    @Override
    public VectorWriter fieldsWriter(SegmentWriteState state) throws IOException {
      VectorWriter writer = delegate.fieldsWriter(state);
      return new VectorWriter() {
        @Override
        public void writeField(FieldInfo fieldInfo, VectorValues values) throws IOException {
          fieldsWritten.add(fieldInfo.name);
          writer.writeField(fieldInfo, values);
        }

        @Override
        public void finish() throws IOException {
          writer.finish();
        }

        @Override
        public void close() throws IOException {
          writer.close();
        }
      };
    }

    @Override
    public VectorReader fieldsReader(SegmentReadState state) throws IOException {
      return delegate.fieldsReader(state);
    }
  }
}
