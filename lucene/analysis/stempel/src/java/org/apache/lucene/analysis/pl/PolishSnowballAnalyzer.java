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
package org.apache.lucene.analysis.pl;

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.tartarus.snowball.ext.PolishStemmer;

/** An {@link Analyzer} for the Polish language based on the built-in snowball stemmer. */
public final class PolishSnowballAnalyzer extends StopwordAnalyzerBase {
  /** File containing default Polish stopwords. */
  public static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  private final CharArraySet stemExclusionSet;

  /**
   * Returns an unmodifiable instance of the default stop words set.
   *
   * @return default stop words set.
   */
  public static CharArraySet getDefaultStopSet() {
    return PolishAnalyzer.getDefaultStopSet();
  }

  /** Builds an analyzer with the default stop words: {@link #DEFAULT_STOPWORD_FILE}. */
  public PolishSnowballAnalyzer() {
    this(getDefaultStopSet());
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords a stopword set
   */
  public PolishSnowballAnalyzer(CharArraySet stopwords) {
    this(stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is provided
   * this analyzer will add a {@link SetKeywordMarkerFilter} before stemming.
   *
   * @param stopwords a stopword set
   * @param stemExclusionSet a set of terms not to be stemmed
   */
  public PolishSnowballAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
    super(stopwords);
    this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
  }

  /**
   * Creates a {@link TokenStreamComponents} which tokenizes all the text in the provided {@link
   * Reader}.
   *
   * @return A {@link TokenStreamComponents} built from an {@link StandardTokenizer} filtered with
   *     {@link LowerCaseFilter}, {@link StopFilter} , {@link SetKeywordMarkerFilter} if a stem
   *     exclusion set is provided and {@link PolishStemmer}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new LowerCaseFilter(source);
    result = new StopFilter(result, stopwords);
    if (!stemExclusionSet.isEmpty()) result = new SetKeywordMarkerFilter(result, stemExclusionSet);
    result = new SnowballFilter(result, new PolishStemmer());
    return new TokenStreamComponents(source, result);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
