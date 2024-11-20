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
package org.apache.lucene.analysis.ja;

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthCharFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ja.dict.UserDictionary;

/**
 * Analyzer for Japanese completion suggester.
 *
 * @see JapaneseCompletionFilter
 */
public class JapaneseCompletionAnalyzer extends Analyzer {
  private final JapaneseCompletionFilter.Mode mode;
  private final UserDictionary userDict;

  /** Creates a new {@code JapaneseCompletionAnalyzer} with default configurations */
  public JapaneseCompletionAnalyzer() {
    this(null, JapaneseCompletionFilter.Mode.INDEX);
  }

  /** Creates a new {@code JapaneseCompletionAnalyzer} */
  public JapaneseCompletionAnalyzer(UserDictionary userDict, JapaneseCompletionFilter.Mode mode) {
    this.userDict = userDict;
    this.mode = mode;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer =
        new JapaneseTokenizer(userDict, true, true, JapaneseTokenizer.Mode.NORMAL);
    TokenStream stream = new JapaneseCompletionFilter(tokenizer, mode);
    stream = new LowerCaseFilter(stream);
    return new TokenStreamComponents(tokenizer, stream);
  }

  @Override
  protected Reader initReader(String fieldName, Reader reader) {
    return new CJKWidthCharFilter(reader);
  }

  @Override
  protected Reader initReaderForNormalization(String fieldName, Reader reader) {
    return new CJKWidthCharFilter(reader);
  }
}
