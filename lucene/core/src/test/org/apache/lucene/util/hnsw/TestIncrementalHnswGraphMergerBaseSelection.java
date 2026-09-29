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
package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests which source graph {@link IncrementalHnswGraphMerger} picks as the base graph when some
 * candidates carry deletions.
 */
public class TestIncrementalHnswGraphMergerBaseSelection extends LuceneTestCase {

  private static final int DIM = 16;
  private static final String FIELD = "v";

  /**
   * Segment A has 1000 nodes of which 300 are deleted (700 live, 30% deleted, under the 40%
   * threshold); segment B has 900 nodes and no deletions. B holds more live vectors, so it must be
   * chosen as the base regardless of the order in which the readers are added.
   */
  public void testBaseGraphChosenByLiveCount() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(16, 100, 0)));
      cfg.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        addSegment(w, "a", 1000);
        addSegment(w, "b", 900);
        for (int i = 0; i < 300; i++) {
          w.deleteDocuments(new Term("id", "a" + i));
        }
        w.commit();
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(2, leaves.size());
        CodecReader a = segment(leaves, 1000);
        CodecReader b = segment(leaves, 900);
        assertEquals(700, a.numDocs());
        assertEquals(900, b.numDocs());
        FieldInfo fieldInfo = a.getFieldInfos().fieldInfo(FIELD);

        for (boolean aFirst : new boolean[] {true, false}) {
          for (boolean concurrent : new boolean[] {false, true}) {
            IncrementalHnswGraphMerger merger =
                concurrent
                    ? new ConcurrentHnswMerger(fieldInfo, null, 16, 100, null, 2)
                    : new IncrementalHnswGraphMerger(fieldInfo, null, 16, 100);
            CodecReader first = aFirst ? a : b;
            CodecReader second = aFirst ? b : a;
            merger.addReader(first.getVectorReader(), doc -> doc, first.getLiveDocs());
            merger.addReader(second.getVectorReader(), doc -> doc, second.getLiveDocs());
            assertNotNull(merger.largestGraphReader);
            assertEquals(
                "aFirst=" + aFirst + " concurrent=" + concurrent,
                900,
                merger.largestGraphReader.graphSize());
          }
        }
      }
    }
  }

  private static CodecReader segment(List<LeafReaderContext> leaves, int maxDoc) {
    for (LeafReaderContext ctx : leaves) {
      if (ctx.reader().maxDoc() == maxDoc) {
        return (CodecReader) ctx.reader();
      }
    }
    throw new AssertionError("no segment with maxDoc=" + maxDoc);
  }

  private void addSegment(IndexWriter w, String prefix, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", prefix + i, Field.Store.NO));
      float[] v = new float[DIM];
      for (int j = 0; j < DIM; j++) {
        v[j] = random().nextFloat();
      }
      doc.add(new KnnFloatVectorField(FIELD, v));
      w.addDocument(doc);
    }
    w.flush();
  }
}
