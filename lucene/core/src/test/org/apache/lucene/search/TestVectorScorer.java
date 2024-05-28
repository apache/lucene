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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestVectorScorer extends LuceneTestCase {

  public void testFindAll() throws IOException {
    VectorEncoding encoding = RandomPicks.randomFrom(random(), VectorEncoding.values());
    try (Directory indexStore =
            getIndexStore(
                "field", encoding, new float[] {0, 1}, new float[] {1, 2}, new float[] {0, 0});
        IndexReader reader = DirectoryReader.open(indexStore)) {
      assert reader.leaves().size() == 1;
      LeafReaderContext context = reader.leaves().get(0);
      final VectorScorer vectorScorer;
      switch (encoding) {
        case BYTE:
          vectorScorer = context.reader().getByteVectorValues("field").scorer(new byte[] {1, 2});
          break;
        case FLOAT32:
          vectorScorer = context.reader().getFloatVectorValues("field").scorer(new float[] {1, 2});
          break;
        default:
          throw new IllegalArgumentException("unexpected vector encoding: " + encoding);
      }

      DocIdSetIterator iterator = vectorScorer.iterator();
      int numDocs = 0;
      while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        numDocs++;
      }
      assertEquals(3, numDocs);
    }
  }

  /** Creates a new directory and adds documents with the given vectors as kNN vector fields */
  private Directory getIndexStore(String field, VectorEncoding encoding, float[]... contents)
      throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      if (encoding == VectorEncoding.BYTE) {
        byte[] v = new byte[contents[i].length];
        for (int j = 0; j < v.length; j++) {
          v[j] = (byte) contents[i][j];
        }
        doc.add(new KnnByteVectorField(field, v, EUCLIDEAN));
      } else {
        doc.add(new KnnFloatVectorField(field, contents[i]));
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
