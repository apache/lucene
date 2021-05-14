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

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.index.BaseVectorFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

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

  // just a simple trivial test
  public void testTwoFieldsTwoFormats() throws IOException {
    Analyzer analyzer = new MockAnalyzer(random());

    try (Directory directory = newDirectory()) {
      // we don't use RandomIndexWriter because it might add more values than we expect !!!!1
      IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
      final VectorFormat fast = TestUtil.getDefaultVectorFormat();
      final VectorFormat slow = VectorFormat.forName("Asserting");
      iwc.setCodec(
          new AssertingCodec() {
            @Override
            public VectorFormat getVectorFormatForField(String field) {
              if ("v1".equals(field)) {
                return fast;
              } else {
                return slow;
              }
            }
          });
      try (IndexWriter iwriter = new IndexWriter(directory, iwc)) {
        Document doc = new Document();
        doc.add(newTextField("id", "1", Field.Store.YES));
        doc.add(new VectorField("v1", new float[] {1, 2, 3}));
        iwriter.addDocument(doc);
        doc = new Document();
        doc.add(newTextField("id", "2", Field.Store.YES));
        doc.add(new VectorField("v2", new float[] {4, 5, 6}));
        iwriter.addDocument(doc);
      }

      // Now search the index:
      try (IndexReader ireader = DirectoryReader.open(directory)) {
        TopDocs hits1 =
            ireader
                .leaves()
                .get(0)
                .reader()
                .searchNearestVectors("v1", new float[] {1, 2, 3}, 10, 1);
        assertEquals(1, hits1.scoreDocs.length);
        TopDocs hits2 =
            ireader
                .leaves()
                .get(0)
                .reader()
                .searchNearestVectors("v2", new float[] {1, 2, 3}, 10, 1);
        assertEquals(1, hits2.scoreDocs.length);
      }
    }
  }
}
