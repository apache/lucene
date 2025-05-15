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
package org.apache.lucene.analysis.ko.dict;

import org.apache.lucene.analysis.ko.POS;

/** Morphological information for user dictionary. */
final class UserMorphData implements KoMorphData {
  private static final int WORD_COST = -100000;

  // NNG left
  private static final short LEFT_ID = 1781;

  // length, length... indexed by compound ID or null for simple noun
  private final int[][] segmentations;
  private final short[] rightIds;

  UserMorphData(int[][] segmentations, short[] rightIds) {
    this.segmentations = segmentations;
    this.rightIds = rightIds;
  }

  @Override
  public int getLeftId(int morphId) {
    return LEFT_ID;
  }

  @Override
  public int getRightId(int morphId) {
    return rightIds[morphId];
  }

  @Override
  public int getWordCost(int morphId) {
    return WORD_COST;
  }

  @Override
  public POS.Type getPOSType(int morphId) {
    if (segmentations[morphId] == null) {
      return POS.Type.MORPHEME;
    } else {
      return POS.Type.COMPOUND;
    }
  }

  @Override
  public POS.Tag getLeftPOS(int morphId) {
    return POS.Tag.NNG;
  }

  @Override
  public POS.Tag getRightPOS(int morphId) {
    return POS.Tag.NNG;
  }

  @Override
  public String getReading(int morphId) {
    return null;
  }

  @Override
  public Morpheme[] getMorphemes(int morphId, char[] surfaceForm, int off, int len) {
    int[] segs = segmentations[morphId];
    if (segs == null) {
      return null;
    }
    int offset = 0;
    Morpheme[] morphemes = new Morpheme[segs.length];
    for (int i = 0; i < segs.length; i++) {
      morphemes[i] = new Morpheme(POS.Tag.NNG, new String(surfaceForm, off + offset, segs[i]));
      offset += segs[i];
    }
    return morphemes;
  }
}
