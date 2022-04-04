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
package org.apache.lucene.analysis.morph;

/** Analyzed token with morphological data. */
public abstract class Token {
  protected final char[] surfaceForm;
  protected final int offset;
  protected final int length;

  protected final int startOffset;
  protected final int endOffset;
  protected int posIncr = 1;
  protected int posLen = 1;

  protected Token(char[] surfaceForm, int offset, int length, int startOffset, int endOffset) {
    this.surfaceForm = surfaceForm;
    this.offset = offset;
    this.length = length;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /** @return surfaceForm */
  public char[] getSurfaceForm() {
    return surfaceForm;
  }

  /** @return offset into surfaceForm */
  public int getOffset() {
    return offset;
  }

  /** @return length of surfaceForm */
  public int getLength() {
    return length;
  }

  /** @return surfaceForm as a String */
  public String getSurfaceFormString() {
    return new String(surfaceForm, offset, length);
  }

  /** Get the start offset of the term in the analyzed text. */
  public int getStartOffset() {
    return startOffset;
  }

  /** Get the end offset of the term in the analyzed text. */
  public int getEndOffset() {
    return endOffset;
  }

  public void setPositionIncrement(int posIncr) {
    this.posIncr = posIncr;
  }

  public int getPositionIncrement() {
    return posIncr;
  }

  /**
   * Set the position length (in tokens) of this token. For normal tokens this is 1; for compound
   * tokens it's &gt; 1.
   */
  public void setPositionLength(int posLen) {
    this.posLen = posLen;
  }

  /**
   * Get the length (in tokens) of this token. For normal tokens this is 1; for compound tokens it's
   * &gt; 1.
   */
  public int getPositionLength() {
    return posLen;
  }
}
