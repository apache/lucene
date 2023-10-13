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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.analysis.morph.DictionaryEntryWriter;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;

/** Writes system dictionary entries. */
class TokenInfoDictionaryEntryWriter extends DictionaryEntryWriter {
  private static final int ID_LIMIT = 8192;

  TokenInfoDictionaryEntryWriter(int size) {
    super(size);
  }

  /**
   * put the entry in map
   *
   * <p>mecab-ko-dic features
   *
   * <pre>
   * 0   - surface
   * 1   - left cost
   * 2   - right cost
   * 3   - word cost
   * 4   - part of speech0+part of speech1+...
   * 5   - semantic class
   * 6   - T if the last character of the surface form has a coda, F otherwise
   * 7   - reading
   * 8   - POS type (*, Compound, Inflect, Preanalysis)
   * 9   - left POS
   * 10  - right POS
   * 11  - expression
   * </pre>
   *
   * @return current position of buffer, which will be wordId of next entry
   */
  @Override
  protected int putEntry(String[] entry) {
    short leftId = Short.parseShort(entry[1]);
    short rightId = Short.parseShort(entry[2]);
    short wordCost = Short.parseShort(entry[3]);

    final POS.Type posType = POS.resolveType(entry[8]);
    final POS.Tag leftPOS;
    final POS.Tag rightPOS;
    if (posType == POS.Type.MORPHEME || posType == POS.Type.COMPOUND || entry[9].equals("*")) {
      leftPOS = POS.resolveTag(entry[4]);
      assert (entry[9].equals("*") && entry[10].equals("*"));
      rightPOS = leftPOS;
    } else {
      leftPOS = POS.resolveTag(entry[9]);
      rightPOS = POS.resolveTag(entry[10]);
    }
    final String reading = entry[7].equals("*") ? "" : entry[0].equals(entry[7]) ? "" : entry[7];
    final String expression = entry[11].equals("*") ? "" : entry[11];

    // extend buffer if necessary
    int left = buffer.remaining();
    // worst case, 3 short + 4 bytes and features (all as utf-16)
    int worstCase = 9 + 2 * (expression.length() + reading.length());
    if (worstCase > left) {
      ByteBuffer newBuffer =
          ByteBuffer.allocateDirect(ArrayUtil.oversize(buffer.limit() + worstCase - left, 1));
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }

    // add pos mapping
    int toFill = 1 + leftId - posDict.size();
    for (int i = 0; i < toFill; i++) {
      posDict.add(null);
    }
    String fullPOSData = leftPOS.name() + "," + entry[5];
    String existing = posDict.get(leftId);
    assert existing == null || existing.equals(fullPOSData);
    posDict.set(leftId, fullPOSData);

    final List<KoMorphData.Morpheme> morphemes = new ArrayList<>();
    // true if the POS and decompounds of the token are all the same.
    boolean hasSinglePOS = (leftPOS == rightPOS);
    if (posType != POS.Type.MORPHEME && expression.length() > 0) {
      String[] exprTokens = expression.split("\\+");
      for (String exprToken : exprTokens) {
        String[] tokenSplit = exprToken.split("/");
        assert tokenSplit.length == 3;
        String surfaceForm = tokenSplit[0].trim();
        if (surfaceForm.isEmpty() == false) {
          POS.Tag exprTag = POS.resolveTag(tokenSplit[1]);
          morphemes.add(new KoMorphData.Morpheme(exprTag, tokenSplit[0]));
          if (leftPOS != exprTag) {
            hasSinglePOS = false;
          }
        }
      }
    }

    int flags = 0;
    if (hasSinglePOS) {
      flags |= TokenInfoMorphData.HAS_SINGLE_POS;
    }
    if (posType == POS.Type.MORPHEME && reading.length() > 0) {
      flags |= TokenInfoMorphData.HAS_READING;
    }

    if (leftId >= ID_LIMIT) {
      throw new IllegalArgumentException("leftId >= " + ID_LIMIT + ": " + leftId);
    }
    if (posType.ordinal() >= 4) {
      throw new IllegalArgumentException("posType.ordinal() >= " + 4 + ": " + posType.name());
    }
    buffer.putShort((short) (leftId << 2 | posType.ordinal()));
    buffer.putShort((short) (rightId << 2 | flags));
    buffer.putShort(wordCost);

    if (posType == POS.Type.MORPHEME) {
      assert leftPOS == rightPOS;
      if (reading.length() > 0) {
        writeString(reading);
      }
    } else {
      if (hasSinglePOS == false) {
        buffer.put((byte) rightPOS.ordinal());
      }
      buffer.put((byte) morphemes.size());
      int compoundOffset = 0;
      for (KoMorphData.Morpheme morpheme : morphemes) {
        if (hasSinglePOS == false) {
          buffer.put((byte) morpheme.posTag.ordinal());
        }
        if (posType != POS.Type.INFLECT) {
          buffer.put((byte) morpheme.surfaceForm.length());
          compoundOffset += morpheme.surfaceForm.length();
        } else {
          writeString(morpheme.surfaceForm);
        }
        assert compoundOffset <= entry[0].length() : Arrays.toString(entry);
      }
    }
    return buffer.position();
  }

  private void writeString(String s) {
    buffer.put((byte) s.length());
    for (int i = 0; i < s.length(); i++) {
      buffer.putChar(s.charAt(i));
    }
  }

  @Override
  protected void writePosDict(OutputStream bos, DataOutput out) throws IOException {
    out.writeVInt(posDict.size());
    for (String s : posDict) {
      if (s == null) {
        out.writeByte((byte) POS.Tag.UNKNOWN.ordinal());
      } else {
        String[] data = CSVUtil.parse(s);
        if (data.length != 2) {
          throw new IllegalArgumentException(
              "Malformed pos/inflection: " + s + "; expected 2 characters");
        }
        out.writeByte((byte) POS.Tag.valueOf(data[0]).ordinal());
      }
    }
  }
}
