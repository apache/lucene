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

import org.apache.lucene.analysis.ja.JapaneseTokenizer.Type;
import org.apache.lucene.analysis.ja.dict.JaMorphData;

/** Analyzed token with morphological data from its dictionary. */
public class Token extends org.apache.lucene.analysis.morph.Token {
  private final JaMorphData morphData;

  private final int morphId;

  private final Type type;

  public Token(
      char[] surfaceForm,
      int offset,
      int length,
      int startOffset,
      int endOffset,
      int morphId,
      Type type,
      JaMorphData morphData) {
    super(surfaceForm, offset, length, startOffset, endOffset);
    this.morphId = morphId;
    this.type = type;
    this.morphData = morphData;
  }

  @Override
  public String toString() {
    return "Token(\""
        + new String(surfaceForm, offset, length)
        + "\" offset="
        + startOffset
        + " length="
        + length
        + " posLen="
        + posLen
        + " type="
        + type
        + " morphId="
        + morphId
        + " leftID="
        + morphData.getLeftId(morphId)
        + ")";
  }

  /** @return reading. null if token doesn't have reading. */
  public String getReading() {
    return morphData.getReading(morphId, surfaceForm, offset, length);
  }

  /** @return pronunciation. null if token doesn't have pronunciation. */
  public String getPronunciation() {
    return morphData.getPronunciation(morphId, surfaceForm, offset, length);
  }

  /** @return part of speech. */
  public String getPartOfSpeech() {
    return morphData.getPartOfSpeech(morphId);
  }

  /** @return inflection type or null */
  public String getInflectionType() {
    return morphData.getInflectionType(morphId);
  }

  /** @return inflection form or null */
  public String getInflectionForm() {
    return morphData.getInflectionForm(morphId);
  }

  /** @return base form or null if token is not inflected */
  public String getBaseForm() {
    return morphData.getBaseForm(morphId, surfaceForm, offset, length);
  }

  /**
   * Returns the type of this token
   *
   * @return token type, not null
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns true if this token is known word
   *
   * @return true if this token is in standard dictionary. false if not.
   */
  public boolean isKnown() {
    return type == Type.KNOWN;
  }

  /**
   * Returns true if this token is unknown word
   *
   * @return true if this token is unknown word. false if not.
   */
  public boolean isUnknown() {
    return type == Type.UNKNOWN;
  }

  /**
   * Returns true if this token is defined in user dictionary
   *
   * @return true if this token is in user dictionary. false if not.
   */
  public boolean isUser() {
    return type == Type.USER;
  }
}
