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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;

/** Morphological information for system dictionary. */
class TokenInfoMorphData implements JaMorphData {

  private final ByteBuffer buffer;
  private final String[] posDict;
  private final String[] inflTypeDict;
  private final String[] inflFormDict;

  TokenInfoMorphData(ByteBuffer buffer, IOSupplier<InputStream> posResource) throws IOException {
    this.buffer = buffer;
    try (InputStream posIS = new BufferedInputStream(posResource.get())) {
      final DataInput in = new InputStreamDataInput(posIS);
      CodecUtil.checkHeader(
          in,
          DictionaryConstants.POSDICT_HEADER,
          DictionaryConstants.VERSION,
          DictionaryConstants.VERSION);
      final int posSize = in.readVInt();
      this.posDict = new String[posSize];
      this.inflTypeDict = new String[posSize];
      this.inflFormDict = new String[posSize];
      populatePosDict(in, posSize, posDict, inflTypeDict, inflFormDict);
    }
  }

  private static void populatePosDict(
      DataInput in, int posSize, String[] posDict, String[] inflTypeDict, String[] inflFormDict)
      throws IOException {
    for (int j = 0; j < posSize; j++) {
      posDict[j] = in.readString();
      inflTypeDict[j] = in.readString();
      inflFormDict[j] = in.readString();
      // this is how we encode null inflections
      if (inflTypeDict[j].length() == 0) {
        inflTypeDict[j] = null;
      }
      if (inflFormDict[j].length() == 0) {
        inflFormDict[j] = null;
      }
    }
  }

  @Override
  public int getLeftId(int morphId) {
    return (buffer.getShort(morphId) & 0xffff) >>> 3;
  }

  @Override
  public int getRightId(int morphId) {
    return (buffer.getShort(morphId) & 0xffff) >>> 3;
  }

  @Override
  public int getWordCost(int morphId) {
    return buffer.getShort(morphId + 2); // Skip id
  }

  @Override
  public String getBaseForm(int morphId, char[] surfaceForm, int off, int len) {
    if (hasBaseFormData(morphId)) {
      int offset = baseFormOffset(morphId);
      int data = buffer.get(offset++) & 0xff;
      int prefix = data >>> 4;
      int suffix = data & 0xF;
      char[] text = new char[prefix + suffix];
      System.arraycopy(surfaceForm, off, text, 0, prefix);
      for (int i = 0; i < suffix; i++) {
        text[prefix + i] = buffer.getChar(offset + (i << 1));
      }
      return new String(text);
    } else {
      return null;
    }
  }

  @Override
  public String getReading(int morphId, char[] surface, int off, int len) {
    if (hasReadingData(morphId)) {
      int offset = readingOffset(morphId);
      int readingData = buffer.get(offset++) & 0xff;
      return readString(offset, readingData >>> 1, (readingData & 1) == 1);
    } else {
      // the reading is the surface form, with hiragana shifted to katakana
      char[] text = new char[len];
      for (int i = 0; i < len; i++) {
        char ch = surface[off + i];
        if (ch > 0x3040 && ch < 0x3097) {
          text[i] = (char) (ch + 0x60);
        } else {
          text[i] = ch;
        }
      }
      return new String(text);
    }
  }

  @Override
  public String getPartOfSpeech(int morphId) {
    return posDict[getLeftId(morphId)];
  }

  @Override
  public String getPronunciation(int morphId, char[] surface, int off, int len) {
    if (hasPronunciationData(morphId)) {
      int offset = pronunciationOffset(morphId);
      int pronunciationData = buffer.get(offset++) & 0xff;
      return readString(offset, pronunciationData >>> 1, (pronunciationData & 1) == 1);
    } else {
      return getReading(morphId, surface, off, len); // same as the reading
    }
  }

  @Override
  public String getInflectionType(int morphId) {
    return inflTypeDict[getLeftId(morphId)];
  }

  @Override
  public String getInflectionForm(int wordId) {
    return inflFormDict[getLeftId(wordId)];
  }

  private int readingOffset(int wordId) {
    int offset = baseFormOffset(wordId);
    if (hasBaseFormData(wordId)) {
      int baseFormLength = buffer.get(offset++) & 0xf;
      return offset + (baseFormLength << 1);
    } else {
      return offset;
    }
  }

  private int pronunciationOffset(int wordId) {
    if (hasReadingData(wordId)) {
      int offset = readingOffset(wordId);
      int readingData = buffer.get(offset++) & 0xff;
      final int readingLength;
      if ((readingData & 1) == 0) {
        readingLength = readingData & 0xfe; // UTF-16: mask off kana bit
      } else {
        readingLength = readingData >>> 1;
      }
      return offset + readingLength;
    } else {
      return readingOffset(wordId);
    }
  }

  private static int baseFormOffset(int wordId) {
    return wordId + 4;
  }

  private boolean hasBaseFormData(int wordId) {
    return (buffer.getShort(wordId) & HAS_BASEFORM) != 0;
  }

  private boolean hasReadingData(int wordId) {
    return (buffer.getShort(wordId) & HAS_READING) != 0;
  }

  private boolean hasPronunciationData(int wordId) {
    return (buffer.getShort(wordId) & HAS_PRONUNCIATION) != 0;
  }

  private String readString(int offset, int length, boolean kana) {
    char[] text = new char[length];
    if (kana) {
      for (int i = 0; i < length; i++) {
        text[i] = (char) (0x30A0 + (buffer.get(offset + i) & 0xff));
      }
    } else {
      for (int i = 0; i < length; i++) {
        text[i] = buffer.getChar(offset + (i << 1));
      }
    }
    return new String(text);
  }

  /** flag that the entry has baseform data. otherwise it's not inflected (same as surface form) */
  public static final int HAS_BASEFORM = 1;
  /**
   * flag that the entry has reading data. otherwise reading is surface form converted to katakana
   */
  public static final int HAS_READING = 2;
  /** flag that the entry has pronunciation data. otherwise pronunciation is the reading */
  public static final int HAS_PRONUNCIATION = 4;
}
