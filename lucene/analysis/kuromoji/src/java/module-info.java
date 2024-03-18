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

/** Japanese Morphological Analyzer */
module org.apache.lucene.analysis.kuromoji {
  requires org.apache.lucene.core;
  requires org.apache.lucene.analysis.common;

  exports org.apache.lucene.analysis.ja;
  exports org.apache.lucene.analysis.ja.completion;
  exports org.apache.lucene.analysis.ja.dict;
  exports org.apache.lucene.analysis.ja.tokenattributes;
  exports org.apache.lucene.analysis.ja.util;

  opens org.apache.lucene.analysis.ja to
      org.apache.lucene.core;
  opens org.apache.lucene.analysis.ja.completion to
      org.apache.lucene.core;

  provides org.apache.lucene.analysis.CharFilterFactory with
      org.apache.lucene.analysis.ja.JapaneseIterationMarkCharFilterFactory;
  provides org.apache.lucene.analysis.TokenizerFactory with
      org.apache.lucene.analysis.ja.JapaneseTokenizerFactory;
  provides org.apache.lucene.analysis.TokenFilterFactory with
      org.apache.lucene.analysis.ja.JapaneseBaseFormFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseCompletionFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseNumberFilterFactory,
      org.apache.lucene.analysis.ja.JapanesePartOfSpeechStopFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseReadingFormFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseHiraganaUppercaseFilterFactory,
      org.apache.lucene.analysis.ja.JapaneseKatakanaUppercaseFilterFactory;
}
