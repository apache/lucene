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
package org.apache.lucene.analysis.ja.dict;

import org.apache.lucene.analysis.morph.MorphData;

/** Represents Japanese morphological information. */
public interface JaMorphData extends MorphData {
  /**
   * Get Part-Of-Speech of tokens
   *
   * @param morphId word ID of token
   * @return Part-Of-Speech of the token
   */
  String getPartOfSpeech(int morphId);

  /**
   * Get reading of tokens
   *
   * @param morphId word ID of token
   * @return Reading of the token
   */
  String getReading(int morphId, char[] surface, int off, int len);

  /**
   * Get base form of word
   *
   * @param morphId word ID of token
   * @return Base form (only different for inflected words, otherwise null)
   */
  String getBaseForm(int morphId, char[] surface, int off, int len);

  /**
   * Get pronunciation of tokens
   *
   * @param morphId word ID of token
   * @return Pronunciation of the token
   */
  String getPronunciation(int morphId, char[] surface, int off, int len);

  /**
   * Get inflection type of tokens
   *
   * @param morphId word ID of token
   * @return inflection type, or null
   */
  String getInflectionType(int morphId);

  /**
   * Get inflection form of tokens
   *
   * @param wordId word ID of token
   * @return inflection form, or null
   */
  String getInflectionForm(int wordId);
  // TODO: maybe we should have a optimal method, a non-typesafe
  // 'getAdditionalData' if other dictionaries like unidic have additional data
}
