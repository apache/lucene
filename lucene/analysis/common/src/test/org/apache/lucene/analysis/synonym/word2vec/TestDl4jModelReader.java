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

package org.apache.lucene.analysis.synonym.word2vec;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

public class TestDl4jModelReader extends LuceneTestCase {

  private static final String MODEL_FILE = "word2vec-model.zip";
  private static final String MODEL_EMPTY_FILE = "word2vec-empty-model.zip";
  private static final String CORRUPTED_VECTOR_DIMENSION_MODEL_FILE =
      "word2vec-corrupted-vector-dimension-model.zip";

  InputStream stream = TestDl4jModelReader.class.getResourceAsStream(MODEL_FILE);
  Dl4jModelReader unit = new Dl4jModelReader(stream);

  @Test
  public void read_zipFileWithMetadata_shouldReturnDictionarySize() throws Exception {
    Word2VecModel model = unit.read();
    long expectedDictionarySize = 235;
    assertEquals(expectedDictionarySize, model.size());
  }

  @Test
  public void read_zipFileWithMetadata_shouldReturnVectorLength() throws Exception {
    Word2VecModel model = unit.read();
    int expectedVectorDimension = 100;
    assertEquals(expectedVectorDimension, model.dimension());
  }

  @Test
  public void read_zipFile_shouldReturnDecodedTerm() throws Exception {
    Word2VecModel model = unit.read();
    BytesRef expectedDecodedFirstTerm = new BytesRef("it");
    assertEquals(expectedDecodedFirstTerm, model.termValue(0));
  }

  @Test
  public void decodeTerm_encodedTerm_shouldReturnDecodedTerm() throws Exception {
    byte[] originalInput = "lucene".getBytes(StandardCharsets.UTF_8);
    String B64encodedLuceneTerm = Base64.getEncoder().encodeToString(originalInput);
    String word2vecEncodedLuceneTerm = "B64:" + B64encodedLuceneTerm;
    assertEquals(new BytesRef("lucene"), Dl4jModelReader.decodeB64Term(word2vecEncodedLuceneTerm));
  }

  @Test
  public void read_EmptyZipFile_shouldThrowException() throws Exception {
    try (InputStream stream = TestDl4jModelReader.class.getResourceAsStream(MODEL_EMPTY_FILE)) {
      Dl4jModelReader unit = new Dl4jModelReader(stream);
      expectThrows(IllegalArgumentException.class, unit::read);
    }
  }

  @Test
  public void read_corruptedVectorDimensionModelFile_shouldThrowException() throws Exception {
    try (InputStream stream =
        TestDl4jModelReader.class.getResourceAsStream(CORRUPTED_VECTOR_DIMENSION_MODEL_FILE)) {
      Dl4jModelReader unit = new Dl4jModelReader(stream);
      expectThrows(RuntimeException.class, unit::read);
    }
  }
}
