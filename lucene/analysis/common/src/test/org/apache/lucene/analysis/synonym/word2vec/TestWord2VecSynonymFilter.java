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

    Word2VecSynonymProvider synonymProvider = new Word2VecSynonymProvider(model);

    Analyzer a = getAnalyzer(synonymProvider, maxSynonymPerTerm, minAcceptedSimilarity);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "d", "e", "c", "b", "post"}, // output
        new int[] {0, 4, 4, 4, 4, 4, 6}, // start offset
        new int[] {3, 5, 5, 5, 5, 5, 10}, // end offset
        new String[] {"word", "word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word"}, // types
        new int[] {1, 1, 0, 0, 0, 0, 1}, // posIncrements
        new int[] {1, 1, 1, 1, 1, 1, 1}); // posLenghts
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

    Word2VecSynonymProvider synonymProvider = new Word2VecSynonymProvider(model);

    Analyzer a = getAnalyzer(synonymProvider, maxSynonymPerTerm, minAcceptedSimilarity);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "d", "e", "post"}, // output
        new int[] {0, 4, 4, 4, 6}, // start offset
        new int[] {3, 5, 5, 5, 10}, // end offset
        new String[] {"word", "word", "SYNONYM", "SYNONYM", "word"}, // types
        new int[] {1, 1, 0, 0, 1}, // posIncrements
        new int[] {1, 1, 1, 1, 1}); // posLenghts
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

    Word2VecSynonymProvider synonymProvider = new Word2VecSynonymProvider(model);

    Analyzer a = getAnalyzer(synonymProvider, 10, 0.9f);
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
        new int[] {1, 1, 1, 1, 1, 1, 1, 1}); // posLengths
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

    Word2VecSynonymProvider synonymProvider = new Word2VecSynonymProvider(model);

    Analyzer a = getAnalyzer(synonymProvider, 10, 0.8f);
    assertAnalyzesTo(
        a,
        "pre a post", // input
        new String[] {"pre", "a", "post"}, // output
        new int[] {0, 4, 6}, // start offset
        new int[] {3, 5, 10}, // end offset
        new String[] {"word", "word", "word"}, // types
        new int[] {1, 1, 1}, // posIncrements
        new int[] {1, 1, 1}); // posLengths
    a.close();
  }

  private Analyzer getAnalyzer(
      Word2VecSynonymProvider synonymProvider,
      int maxSynonymsPerTerm,
      float minAcceptedSimilarity) {
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
