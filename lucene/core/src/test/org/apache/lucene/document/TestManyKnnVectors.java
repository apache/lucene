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
package org.apache.lucene.document;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Monster;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


/**
 * Tests a large dataset of kNN vectors to check for issues that only show up when
 * segments are very large, like overflow. The dataset is based on the StackOverflow
 * track from Elasticsearch's rally benchmarks: https://github.com/elastic/rally-tracks/tree/master/so_vector.
 *
 * Steps to run the test
 *   1. Download the dataset: wget https://rally-tracks.elastic.co/so_vector/documents.bin
 *   2. Move the dataset to the resources folder: mv documents.bin lucene/core/src/resources/
 *   3. Start the test:
 *     ./gradlew test --tests TestManyKnnVectors.testLargeSegment -Dtests.monster=true -Dtests.verbose=true \
 *       -Dorg.gradle.jvmargs="-Xms2g -Xmx2g" --max-workers=1
 */
@TimeoutSuite(millis = 10_800_000) // 3 hour timeout
@Monster("takes ~2 hours and needs 2GB heap")
public class TestManyKnnVectors extends LuceneTestCase {
  public void testLargeSegment() throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec()); // Make sure to use the default codec instead of a random one
    iwc.setRAMBufferSizeMB(3_000); // Use a 3GB buffer to create a single large segment

    String fieldName = "field";
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;

    URL documentsPath = getClass().getClassLoader().getResource("documents.bin");
    assertNotNull(documentsPath);

    try (FileChannel input = FileChannel.open(Paths.get(documentsPath.toURI()));
         Directory dir = FSDirectory.open(createTempDir("ManyKnnVectors"));
         IndexWriter iw = new IndexWriter(dir, iwc)) {

      // This data is enough to trigger the overflow bug in issue #11858,
      // since 1_000_000 * 768 * 4 > Integer.MAX_VALUE
      int numVectors = 1_000_000;
      int dims = 768;

      VectorReader vectorReader = new VectorReader(input, dims);
      for (int i = 0; i < numVectors; i++) {
        float[] vector = vectorReader.next();
        Document doc = new Document();
        doc.add(new KnnVectorField(fieldName, vector, similarityFunction));
        iw.addDocument(doc);
        if (VERBOSE && i % 10_000 == 0) {
          System.out.println("Indexed " + i + " vectors out of " + numVectors);
        }
      }
      iw.forceMerge(1);

      try (IndexReader reader = DirectoryReader.open(iw)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().get(0);

        VectorValues vectorValues = ctx.reader().getVectorValues(fieldName);
        assertNotNull(vectorValues);
        assertEquals(numVectors, vectorValues.size());

        int numVectorsRead = 0;
        while (vectorValues.nextDoc() != NO_MORE_DOCS) {
          float[] v = vectorValues.vectorValue();
          assertEquals(dims, v.length);
          numVectorsRead++;
        }
        assertEquals(numVectors, numVectorsRead);
      }
    }
  }

  private static class VectorReader {
    private final FileChannel input;
    private final float[] vector;
    private final ByteBuffer buffer;

    public VectorReader(FileChannel input, int dims) {
      this.input = input;
      this.vector = new float[dims];

      byte[] bytes = new byte[dims * Float.BYTES];
      this.buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    public float[] next() throws IOException {
      input.read(buffer);
      buffer.position(0);
      FloatBuffer floatBuffer = buffer.asFloatBuffer();
      floatBuffer.get(vector);
      return vector;
    }
  }
}
