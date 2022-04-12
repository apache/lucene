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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;

/** Morphological information for system dictionary. */
class TokenInfoMorphData implements KoMorphData {

  private final ByteBuffer buffer;
  private final POS.Tag[] posDict;

  TokenInfoMorphData(ByteBuffer buffer, IOSupplier<InputStream> posResource) throws IOException {
    this.buffer = buffer;
    try (InputStream posIS = new BufferedInputStream(posResource.get())) {
      DataInput in = new InputStreamDataInput(posIS);
      CodecUtil.checkHeader(
          in,
          DictionaryConstants.POSDICT_HEADER,
          DictionaryConstants.VERSION,
          DictionaryConstants.VERSION);
      int posSize = in.readVInt();
      this.posDict = new POS.Tag[posSize];
      for (int j = 0; j < posSize; j++) {
        posDict[j] = POS.resolveTag(in.readByte());
      }
    }
  }

  @Override
  public int getLeftId(int morphId) {
    return buffer.getShort(morphId) >>> 2;
  }

  @Override
  public int getRightId(int morphId) {
    return buffer.getShort(morphId + 2) >>> 2; // Skip left id
  }

  @Override
  public int getWordCost(int morphId) {
    return buffer.getShort(morphId + 4); // Skip left and right id
  }

  @Override
  public POS.Type getPOSType(int morphId) {
    byte value = (byte) (buffer.getShort(morphId) & 3);
    return POS.resolveType(value);
  }

  @Override
  public POS.Tag getLeftPOS(int morphId) {
    return posDict[getLeftId(morphId)];
  }

  @Override
  public POS.Tag getRightPOS(int morphId) {
    POS.Type type = getPOSType(morphId);
    if (type == POS.Type.MORPHEME || type == POS.Type.COMPOUND || hasSinglePOS(morphId)) {
      return getLeftPOS(morphId);
    } else {
      byte value = buffer.get(morphId + 6);
      return POS.resolveTag(value);
    }
  }

  @Override
  public String getReading(int morphId) {
    if (hasReadingData(morphId)) {
      int offset = morphId + 6;
      return readString(offset);
    }
    return null;
  }

  @Override
  public Morpheme[] getMorphemes(int morphId, char[] surfaceForm, int off, int len) {
    POS.Type posType = getPOSType(morphId);
    if (posType == POS.Type.MORPHEME) {
      return null;
    }
    int offset = morphId + 6;
    boolean hasSinglePos = hasSinglePOS(morphId);
    if (hasSinglePos == false) {
      offset++; // skip rightPOS
    }
    int length = buffer.get(offset++);
    if (length == 0) {
      return null;
    }
    Morpheme[] morphemes = new Morpheme[length];
    int surfaceOffset = 0;
    final POS.Tag leftPOS = getLeftPOS(morphId);
    for (int i = 0; i < length; i++) {
      final String form;
      final POS.Tag tag = hasSinglePos ? leftPOS : POS.resolveTag(buffer.get(offset++));
      if (posType == POS.Type.INFLECT) {
        form = readString(offset);
        offset += form.length() * 2 + 1;
      } else {
        int formLen = buffer.get(offset++);
        form = new String(surfaceForm, off + surfaceOffset, formLen);
        surfaceOffset += formLen;
      }
      morphemes[i] = new Morpheme(tag, form);
    }
    return morphemes;
  }

  private String readString(int offset) {
    int strOffset = offset;
    int len = buffer.get(strOffset++);
    char[] text = new char[len];
    for (int i = 0; i < len; i++) {
      text[i] = buffer.getChar(strOffset + (i << 1));
    }
    return new String(text);
  }

  private boolean hasSinglePOS(int wordId) {
    return (buffer.getShort(wordId + 2) & HAS_SINGLE_POS) != 0;
  }

  private boolean hasReadingData(int wordId) {
    return (buffer.getShort(wordId + 2) & HAS_READING) != 0;
  }

  /** flag that the entry has a single part of speech (leftPOS) */
  public static final int HAS_SINGLE_POS = 1;

  /** flag that the entry has reading data. otherwise reading is surface form */
  public static final int HAS_READING = 2;
}
