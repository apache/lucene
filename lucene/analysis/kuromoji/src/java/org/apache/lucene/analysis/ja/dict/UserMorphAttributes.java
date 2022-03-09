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

import org.apache.lucene.analysis.ja.util.CSVUtil;

import static org.apache.lucene.analysis.ja.dict.UserDictionary.CUSTOM_DICTIONARY_WORD_ID_OFFSET;
import static org.apache.lucene.analysis.ja.dict.UserDictionary.INTERNAL_SEPARATOR;

public class UserMorphAttributes implements JaMorphAttributes  {
  public static final int WORD_COST = -100000;
  public static final int LEFT_ID = 5;
  public static final int RIGHT_ID = 5;

  // holds readings and POS, indexed by wordid
  private final String[] data;

  UserMorphAttributes(String[] data) {
    this.data = data;
  }

  @Override
  public int getLeftId(int wordId) {
    return LEFT_ID;
  }

  @Override
  public int getRightId(int wordId) {
    return RIGHT_ID;
  }

  @Override
  public int getWordCost(int wordId) {
    return WORD_COST;
  }

  public String getReading(int wordId, char[] surface, int off, int len) {
    return getFeature(wordId, 0);
  }

  public String getPartOfSpeech(int wordId) {
    return getFeature(wordId, 1);
  }

  public String getBaseForm(int wordId, char[] surface, int off, int len) {
    return null; // TODO: add support?
  }

  public String getPronunciation(int wordId, char[] surface, int off, int len) {
    return null; // TODO: add support?
  }

  public String getInflectionType(int wordId) {
    return null; // TODO: add support?
  }

  public String getInflectionForm(int wordId) {
    return null; // TODO: add support?
  }

  private String[] getAllFeaturesArray(int wordId) {
    String allFeatures = data[wordId - CUSTOM_DICTIONARY_WORD_ID_OFFSET];
    if (allFeatures == null) {
      return null;
    }

    return allFeatures.split(INTERNAL_SEPARATOR);
  }

  private String getFeature(int wordId, int... fields) {
    String[] allFeatures = getAllFeaturesArray(wordId);
    if (allFeatures == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    if (fields.length == 0) { // All features
      for (String feature : allFeatures) {
        sb.append(CSVUtil.quoteEscape(feature)).append(",");
      }
    } else if (fields.length == 1) { // One feature doesn't need to escape value
      sb.append(allFeatures[fields[0]]).append(",");
    } else {
      for (int field : fields) {
        sb.append(CSVUtil.quoteEscape(allFeatures[field])).append(",");
      }
    }
    return sb.deleteCharAt(sb.length() - 1).toString();
  }

}
