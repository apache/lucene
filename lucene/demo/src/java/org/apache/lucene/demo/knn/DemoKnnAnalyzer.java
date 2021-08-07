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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * This analyzer provides {@link #analyze(String, String)} for calculating "semantic" vectors for
 * input strings.
 */
public class DemoKnnAnalyzer extends Analyzer {

  private final KnnVectorDict dict;

  /**
   * Sole constructor
   *
   * @param dict a token to vector dictionary
   */
  public DemoKnnAnalyzer(KnnVectorDict dict) {
    this.dict = dict;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer = new StandardTokenizer();
    TokenStream output = new KnnVectorDictFilter(new LowerCaseFilter(tokenizer), dict);
    return new TokenStreamComponents(tokenizer, output);
  }

  /**
   * Tokenize and lower-case the input, look up the tokens in the dictionary, and sum the token
   * vectors. Unrecognized tokens are ignored. The resulting vector is normalized to unit length.
   *
   * @param fieldName the field name; ignored
   * @param input the input to analyze
   * @return the KnnVector for the input
   */
  public float[] analyze(String fieldName, String input) throws IOException {
    TokenStream tokens = tokenStream(fieldName, input);
    tokens.reset();
    while (tokens.incrementToken()) {}
    tokens.end();
    float[] result = ((KnnVectorDictFilter) tokens).getResult();
    tokens.close();
    return result;
  }
}
