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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermAndVector;
import org.junit.Test;

public class TestWord2VecSynonymFilter extends BaseTokenStreamTestCase {

  @Test
  public void synonymExpansion_oneCandidate_shouldBeExpandedWithinThreshold() throws Exception {
    int maxSynonymPerTerm = 10;
    float minAcceptedSimilarity = 0.9f;
    Word2VecModel model = new Word2VecModel(6, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {10, 8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {9, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("d"), new float[] {1, 1}));
    model.addTermAndVector(new TermAndVector(new BytesRef("e"), new float[] {99, 101}));
    model.addTermAndVector(new TermAndVector(new BytesRef("f"), new float[] {-1, 10}));

    Word2VecSynonymProvider SynonymProvider = new Word2VecSynonymProvider(model);

    float similarityAWithB = // 0.9969
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(1));
    float similarityAWithC = // 0.9993
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(2));
    float similarityAWithD = // 1.0
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(3));
    float similarityAWithE = // 0.9999
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(4));
    // float similarityAWithF = 0.8166  (not accepted)

    Analyzer a = getAnalyzer(SynonymProvider, maxSynonymPerTerm, minAcceptedSimilarity);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "d", "e", "c", "b", "post"}, // output
        new int[] {0, 4, 4, 4, 4, 4, 6}, // start offset
        new int[] {3, 5, 5, 5, 5, 5, 10}, // end offset
        new String[] {"word", "word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word"}, // types
        new int[] {1, 1, 0, 0, 0, 0, 1}, // posIncrements
        new int[] {1, 1, 1, 1, 1, 1, 1}, // posLenghts
        new float[] {
          1, 1, similarityAWithD, similarityAWithE, similarityAWithC, similarityAWithB, 1
        }); // boost
    a.close();
  }

  @Test
  public void synonymExpansion_oneCandidate_shouldBeExpandedWithTopKSynonyms() throws Exception {
    int maxSynonymPerTerm = 2;
    float minAcceptedSimilarity = 0.9f;
    Word2VecModel model = new Word2VecModel(5, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {10, 8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {9, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("d"), new float[] {1, 1}));
    model.addTermAndVector(new TermAndVector(new BytesRef("e"), new float[] {99, 101}));

    Word2VecSynonymProvider SynonymProvider = new Word2VecSynonymProvider(model);

    // float similarityAWithB = 0.9969  (not in top 2)
    // float similarityAWithC = 0.9993  (not in top 2)
    float similarityAWithD = // 1.0
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(3));
    float similarityAWithE = // 0.9999
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(4));

    Analyzer a = getAnalyzer(SynonymProvider, maxSynonymPerTerm, minAcceptedSimilarity);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "d", "e", "post"}, // output
        new int[] {0, 4, 4, 4, 6}, // start offset
        new int[] {3, 5, 5, 5, 10}, // end offset
        new String[] {"word", "word", "SYNONYM", "SYNONYM", "word"}, // types
        new int[] {1, 1, 0, 0, 1}, // posIncrements
        new int[] {1, 1, 1, 1, 1}, // posLenghts
        new float[] {1, 1, similarityAWithD, similarityAWithE, 1}); // boost
    a.close();
  }

  @Test
  public void synonymExpansion_twoCandidates_shouldBothBeExpanded() throws Exception {
    Word2VecModel model = new Word2VecModel(8, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {10, 8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {9, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("d"), new float[] {1, 1}));
    model.addTermAndVector(new TermAndVector(new BytesRef("e"), new float[] {99, 101}));
    model.addTermAndVector(new TermAndVector(new BytesRef("f"), new float[] {1, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("post"), new float[] {-10, -11}));
    model.addTermAndVector(new TermAndVector(new BytesRef("after"), new float[] {-8, -10}));

    Word2VecSynonymProvider SynonymProvider = new Word2VecSynonymProvider(model);

    float similarityAWithB =
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(1));
    float similarityAWithC =
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(2));
    float similarityAWithD =
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(3));
    float similarityAWithE =
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(0), model.vectorValue(4));
    float similarityPostWithAfter =
        VectorSimilarityFunction.COSINE.compare(model.vectorValue(6), model.vectorValue(7));

    Analyzer a = getAnalyzer(SynonymProvider, 10, 0.9f);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "d", "e", "c", "b", "post", "after"}, // output
        new int[] {0, 4, 4, 4, 4, 4, 6, 6}, // start offset
        new int[] {3, 5, 5, 5, 5, 5, 10, 10}, // end offset
        new String[] { // types
          "word", "word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word", "SYNONYM"
        },
        new int[] {1, 1, 0, 0, 0, 0, 1, 0}, // posIncrements
        new int[] {1, 1, 1, 1, 1, 1, 1, 1}, // posLengths
        new float[] {
          1,
          1,
          similarityAWithD,
          similarityAWithE,
          similarityAWithC,
          similarityAWithB,
          1,
          similarityPostWithAfter
        }); // boost
    a.close();
  }

  @Test
  public void synonymExpansion_forMinAcceptedSimilarity_shouldExpandToNoneSynonyms()
      throws Exception {
    Word2VecModel model = new Word2VecModel(4, 2);
    model.addTermAndVector(new TermAndVector(new BytesRef("a"), new float[] {10, 10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("b"), new float[] {-10, -8}));
    model.addTermAndVector(new TermAndVector(new BytesRef("c"), new float[] {-9, -10}));
    model.addTermAndVector(new TermAndVector(new BytesRef("f"), new float[] {-1, -10}));

    Word2VecSynonymProvider SynonymProvider = new Word2VecSynonymProvider(model);

    Analyzer a = getAnalyzer(SynonymProvider, 10, 0.8f);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "post"}, // output
        new int[] {0, 4, 6}, // start offset
        new int[] {3, 5, 10}, // end offset
        new String[] {"word", "word", "word"}, // types
        new int[] {1, 1, 1}, // posIncrements
        new int[] {1, 1, 1}, // posLengths
        new float[] {1, 1, 1}); // boost
    a.close();
  }

  private Analyzer getAnalyzer(
      SynonymProvider synonymProvider, int maxSynonymsPerTerm, float minAcceptedSimilarity) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        // Make a local variable so testRandomHuge doesn't share it across threads!
        Word2VecSynonymFilter synFilter =
            new Word2VecSynonymFilter(
                tokenizer, synonymProvider, maxSynonymsPerTerm, minAcceptedSimilarity);
        return new TokenStreamComponents(tokenizer, synFilter);
      }
    };
  }
}
