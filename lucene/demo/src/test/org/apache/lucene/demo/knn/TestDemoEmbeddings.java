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
package org.apache.lucene.demo.knn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;

public class TestDemoEmbeddings extends LuceneTestCase {

  public void testComputeEmbedding() throws IOException {
    Path testVectors = getDataPath("../test-files/knn-dict").resolve("knn-token-vectors");
    try (Directory directory = newDirectory()) {
      KnnVectorDict.build(testVectors, directory, "dict");
      try (KnnVectorDict dict = new KnnVectorDict(directory, "dict")) {
        DemoEmbeddings demoEmbeddings = new DemoEmbeddings(dict);

        // test garbage
        float[] garbageVector =
            demoEmbeddings.computeEmbedding("garbagethathasneverbeen seeneverinlife");
        assertEquals(50, garbageVector.length);
        assertArrayEquals(new float[50], garbageVector, 0);

        // test space
        assertArrayEquals(new float[50], demoEmbeddings.computeEmbedding(" "), 0);

        // test some real words that are in the dictionary and some that are not
        float[] realVector = demoEmbeddings.computeEmbedding("the real fact");
        assertEquals(50, realVector.length);

        float[] the = getTermVector(dict, "the");
        assertArrayEquals(new float[50], getTermVector(dict, "real"), 0);
        float[] fact = getTermVector(dict, "fact");
        VectorUtil.add(the, fact);
        VectorUtil.l2normalize(the);
        assertArrayEquals(the, realVector, 0);
      }
    }
  }

  private float[] getTermVector(KnnVectorDict dict, String term) throws IOException {
    byte[] bytes = new byte[200];
    dict.get(new BytesRef(term), bytes);
    float[] vector = new float[50];
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(vector);
    return vector;
  }
}
