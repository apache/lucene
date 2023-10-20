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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermAndVector;
import org.junit.Test;

public class TestWord2VecSynonymProvider extends LuceneTestCase {

  private static final int MAX_SYNONYMS_PER_TERM = 10;
  private static final float MIN_ACCEPTED_SIMILARITY = 0.85f;

  private final Word2VecSynonymProvider unit;

  public TestWord2VecSynonymProvider() throws IOException {
    Word2VecModel model = new Word2VecModel(2, 3);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {0.24f, 0.78f, 0.28f}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {0.44f, 0.01f, 0.81f}));
    unit = new Word2VecSynonymProvider(model);
  }

  @Test
  public void getSynonyms_nullToken_shouldThrowException() {
    expectThrows(
        IllegalArgumentException.class,
        () -> unit.getSynonyms(null, MAX_SYNONYMS_PER_TERM, MIN_ACCEPTED_SIMILARITY));
  }

  @Test
  public void getSynonyms_shouldReturnSynonymsBasedOnMinAcceptedSimilarity() throws Exception {
    Word2VecModel model = new Word2VecModel(6, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {10, 8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {9, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("d"), new float[] {1, 1}));
    model.addTermAndVector(new TermAndVector(new BytesRef("e"), new float[] {99, 101}));
    model.addTermAndVector(new TermAndVector(new BytesRef("f"), new float[] {-1, 10}));

    Word2VecSynonymProvider unit = new Word2VecSynonymProvider(model);

    BytesRef inputTerm = new BytesRef("a");
    String[] expectedSynonyms = {"d", "e", "c", "b"};
    List<TermAndBoost> actualSynonymsResults =
        unit.getSynonyms(inputTerm, MAX_SYNONYMS_PER_TERM, MIN_ACCEPTED_SIMILARITY);

    assertEquals(4, actualSynonymsResults.size());
    for (int i = 0; i < expectedSynonyms.length; i++) {
      assertEquals(new BytesRef(expectedSynonyms[i]), actualSynonymsResults.get(i).term);
    }
  }

  @Test
  public void getSynonyms_shouldReturnSynonymsBoost() throws Exception {
    Word2VecModel model = new Word2VecModel(3, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {1, 1}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {99, 101}));

    Word2VecSynonymProvider unit = new Word2VecSynonymProvider(model);

    BytesRef inputTerm = new BytesRef("a");
    List<TermAndBoost> actualSynonymsResults =
        unit.getSynonyms(inputTerm, MAX_SYNONYMS_PER_TERM, MIN_ACCEPTED_SIMILARITY);

    BytesRef expectedFirstSynonymTerm = new BytesRef("b");
    double expectedFirstSynonymBoost = 1.0;
    assertEquals(expectedFirstSynonymTerm, actualSynonymsResults.get(0).term);
    assertEquals(expectedFirstSynonymBoost, actualSynonymsResults.get(0).boost, 0.001f);
  }

  @Test
  public void noSynonymsWithinAcceptedSimilarity_shouldReturnNoSynonyms() throws Exception {
    Word2VecModel model = new Word2VecModel(4, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {-10, -8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {-9, -10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("d"), new float[] {6, -6}));

    Word2VecSynonymProvider unit = new Word2VecSynonymProvider(model);

    BytesRef inputTerm = newBytesRef("a");
    List<TermAndBoost> actualSynonymsResults =
        unit.getSynonyms(inputTerm, MAX_SYNONYMS_PER_TERM, MIN_ACCEPTED_SIMILARITY);
    assertEquals(0, actualSynonymsResults.size());
  }

  @Test
  public void testModel_shouldReturnNormalizedVectors() {
    Word2VecModel model = new Word2VecModel(4, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {10, 8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {9, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("f"), new float[] {-1, 10}));

    float[] vectorIdA = model.vectorValue(new BytesRef("a"));
    float[] vectorIdF = model.vectorValue(new BytesRef("f"));
    assertArrayEquals(new float[] {0.70710f, 0.70710f}, vectorIdA, 0.001f);
    assertArrayEquals(new float[] {-0.0995f, 0.99503f}, vectorIdF, 0.001f);
  }

  @Test
  public void normalizedVector_shouldReturnModule1() {
    TermAndVector synonymTerm = new TermAndVector(new BytesRef("a"), new float[] {10, 10});
    synonymTerm.normalizeVector();
    float[] vector = synonymTerm.getVector();
    float len = 0;
    for (int i = 0; i < vector.length; i++) {
      len += vector[i] * vector[i];
    }
    assertEquals(1, Math.sqrt(len), 0.0001f);
  }
}
