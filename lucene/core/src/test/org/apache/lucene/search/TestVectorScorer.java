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
package org.apache.lucene.search;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestVectorScorer extends LuceneTestCase {

  public void testFindAll() throws IOException {
    try (Directory indexStore =
            getIndexStore("field", new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      assert reader.leaves().size() == 1;
      LeafReaderContext context = reader.leaves().get(0);
      FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo("field");
      VectorScorer vectorScorer = VectorScorer.create(context, fieldInfo, new float[] {1, 2});

      int numDocs = 0;
      for (int i = 0; i < reader.maxDoc(); i++) {
        if (vectorScorer.advanceExact(i)) {
          numDocs++;
        }
      }
      assertEquals(3, numDocs);
    }
  }

  /** Creates a new directory and adds documents with the given vectors as kNN vector fields */
  private Directory getIndexStore(String field, float[]... contents) throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    VectorEncoding encoding =
        VectorEncoding.values()[random().nextInt(VectorEncoding.values().length)];
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      if (encoding == VectorEncoding.BYTE) {
        BytesRef v = new BytesRef(new byte[contents[i].length]);
        for (int j = 0; j < v.length; j++) {
          v.bytes[j] = (byte) contents[i][j];
        }
        doc.add(new KnnVectorField(field, v, EUCLIDEAN));
      } else {
        doc.add(new KnnVectorField(field, contents[i]));
      }
      doc.add(new StringField("id", "id" + i, Field.Store.YES));
      writer.addDocument(doc);
    }
    // Add some documents without a vector
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StringField("other", "value", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.close();
    return indexStore;
  }
}
