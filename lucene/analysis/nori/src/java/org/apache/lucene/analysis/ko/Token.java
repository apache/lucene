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
package org.apache.lucene.analysis.ko;

import org.apache.lucene.analysis.ko.dict.KoMorphData;

/** Analyzed token with morphological data. */
public abstract class Token extends org.apache.lucene.analysis.morph.Token {

  protected Token(char[] surfaceForm, int offset, int length, int startOffset, int endOffset) {
    super(surfaceForm, offset, length, startOffset, endOffset);
  }

  /** Get the {@link POS.Type} of the token. */
  public abstract POS.Type getPOSType();

  /** Get the left part of speech of the token. */
  public abstract POS.Tag getLeftPOS();

  /** Get the right part of speech of the token. */
  public abstract POS.Tag getRightPOS();

  /** Get the reading of the token. */
  public abstract String getReading();

  /**
   * Get the {@link org.apache.lucene.analysis.ko.dict.KoMorphData.Morpheme} decomposition of the
   * token.
   */
  public abstract KoMorphData.Morpheme[] getMorphemes();
}
