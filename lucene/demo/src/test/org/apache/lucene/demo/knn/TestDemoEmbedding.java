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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

public class TestDemoEmbedding extends LuceneTestCase {

  public void testComputeEmbedding() throws IOException {
    Path testVectors = getDataPath("../test-files/knn-dict").resolve("knn-token-vectors");
    Path dictPath = createTempDir("knn-demo").resolve("dict");
    KnnVectorDict.build(testVectors, dictPath);
    try (KnnVectorDict dict = new KnnVectorDict(dictPath)) {
      DemoEmbedding demoEmbedding = new DemoEmbedding(dict);
      float[] garbageVector =
          demoEmbedding.computeEmbedding("garbagethathasneverbeen seeneverinlife");
      assertEquals(50, garbageVector.length);
      assertArrayEquals(new float[50], garbageVector, 0);

      float[] realVector = demoEmbedding.computeEmbedding("the real fact");
      assertEquals(50, realVector.length);

      float[] the = getTermVector(dict, "the");
      assertNull(dict.get(new BytesRef("real")));
      float[] fact = getTermVector(dict, "fact");
      VectorUtil.add(the, fact);
      VectorUtil.l2normalize(the);
      assertArrayEquals(the, realVector, 0);
    }
  }

  private float[] getTermVector(KnnVectorDict dict, String term) throws IOException {
    byte[] vector = dict.get(new BytesRef(term));
    float[] scratch = new float[50];
    ByteBuffer.wrap(vector).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(scratch);
    return scratch;
  }
}
