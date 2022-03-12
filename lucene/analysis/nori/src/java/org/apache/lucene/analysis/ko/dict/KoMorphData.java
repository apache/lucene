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
import org.apache.lucene.analysis.morph.MorphData;

/** Represents Korean morphological information. */
public interface KoMorphData extends MorphData {
  /** A morpheme extracted from a compound token. */
  class Morpheme {
    public final POS.Tag posTag;
    public final String surfaceForm;

    public Morpheme(POS.Tag posTag, String surfaceForm) {
      this.posTag = posTag;
      this.surfaceForm = surfaceForm;
    }
  }

  /**
   * Get the {@link org.apache.lucene.analysis.ko.POS.Type} of specified word (morpheme, compound,
   * inflect or pre-analysis)
   */
  POS.Type getPOSType(int morphId);

  /**
   * Get the left {@link org.apache.lucene.analysis.ko.POS.Tag} of specfied word.
   *
   * <p>For {@link org.apache.lucene.analysis.ko.POS.Type#MORPHEME} and {@link
   * org.apache.lucene.analysis.ko.POS.Type#COMPOUND} the left and right POS are the same.
   */
  POS.Tag getLeftPOS(int morphId);

  /**
   * Get the right {@link org.apache.lucene.analysis.ko.POS.Tag} of specfied word.
   *
   * <p>For {@link org.apache.lucene.analysis.ko.POS.Type#MORPHEME} and {@link
   * org.apache.lucene.analysis.ko.POS.Type#COMPOUND} the left and right POS are the same.
   */
  POS.Tag getRightPOS(int morphId);

  /** Get the reading of specified word (mainly used for Hanja to Hangul conversion). */
  String getReading(int morphId);

  /** Get the morphemes of specified word (e.g. 가깝으나: 가깝 + 으나). */
  Morpheme[] getMorphemes(int morphId, char[] surfaceForm, int off, int len);
}
