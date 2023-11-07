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
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestKnnVectorDict extends LuceneTestCase {

  public void testBuild() throws IOException {
    Path testVectors = getDataPath("../test-files/knn-dict").resolve("knn-token-vectors");

    try (Directory directory = newDirectory()) {
      KnnVectorDict.build(testVectors, directory, "dict");
      try (KnnVectorDict dict = new KnnVectorDict(directory, "dict")) {
        assertEquals(50, dict.getDimension());
        byte[] vector = new byte[dict.getDimension() * Float.BYTES];

        // not found token has zero vector
        dict.get(new BytesRef("never saw this token"), vector);
        assertArrayEquals(new byte[200], vector);

        // found token has nonzero vector
        dict.get(new BytesRef("the"), vector);
        assertFalse(Arrays.equals(new byte[200], vector));

        // incorrect dimension for output buffer
        expectThrows(
            IllegalArgumentException.class, () -> dict.get(new BytesRef("the"), new byte[10]));
      }
    }
  }
}
