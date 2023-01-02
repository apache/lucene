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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;

/** Base class for a binary-encoded in-memory dictionary. */
public abstract class BinaryDictionary implements Dictionary {

  /** Used to specify where (dictionary) resources get loaded from. */
  @Deprecated(forRemoval = true, since = "9.1")
  public enum ResourceScheme {
    CLASSPATH,
    FILE
  }

  public static final String TARGETMAP_FILENAME_SUFFIX = "$targetMap.dat";
  public static final String DICT_FILENAME_SUFFIX = "$buffer.dat";
  public static final String POSDICT_FILENAME_SUFFIX = "$posDict.dat";

  public static final String DICT_HEADER = "ko_dict";
  public static final String TARGETMAP_HEADER = "ko_dict_map";
  public static final String POSDICT_HEADER = "ko_dict_pos";
  public static final int VERSION = 1;

  private final ByteBuffer buffer;
  private final int[] targetMapOffsets, targetMap;
  private final POS.Tag[] posDict;

  protected BinaryDictionary(
      IOSupplier<InputStream> targetMapResource,
      IOSupplier<InputStream> posResource,
      IOSupplier<InputStream> dictResource)
      throws IOException {
    try (InputStream mapIS = new BufferedInputStream(targetMapResource.get())) {
      DataInput in = new InputStreamDataInput(mapIS);
      CodecUtil.checkHeader(in, TARGETMAP_HEADER, VERSION, VERSION);
      this.targetMap = new int[in.readVInt()];
      this.targetMapOffsets = new int[in.readVInt()];
      populateTargetMap(in, this.targetMap, this.targetMapOffsets);
    }

    try (InputStream posIS = new BufferedInputStream(posResource.get())) {
      DataInput in = new InputStreamDataInput(posIS);
      CodecUtil.checkHeader(in, POSDICT_HEADER, VERSION, VERSION);
      int posSize = in.readVInt();
      this.posDict = new POS.Tag[posSize];
      for (int j = 0; j < posSize; j++) {
        posDict[j] = POS.resolveTag(in.readByte());
      }
    }

    // no buffering here, as we load in one large buffer
    try (InputStream dictIS = dictResource.get()) {
      DataInput in = new InputStreamDataInput(dictIS);
      CodecUtil.checkHeader(in, DICT_HEADER, VERSION, VERSION);
      final int size = in.readVInt();
      final ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size);
      final ReadableByteChannel channel = Channels.newChannel(dictIS);
      final int read = channel.read(tmpBuffer);
      if (read != size) {
        throw new EOFException("Cannot read whole dictionary");
      }
      this.buffer = tmpBuffer.asReadOnlyBuffer();
    }
  }

  private static void populateTargetMap(DataInput in, int[] targetMap, int[] targetMapOffsets)
      throws IOException {
    int accum = 0, sourceId = 0;
    for (int ofs = 0; ofs < targetMap.length; ofs++) {
      final int val = in.readVInt();
      if ((val & 0x01) != 0) {
        targetMapOffsets[sourceId] = ofs;
        sourceId++;
      }
      accum += val >>> 1;
      targetMap[ofs] = accum;
    }
    if (sourceId + 1 != targetMapOffsets.length)
      throw new IOException(
          "targetMap file format broken; targetMap.length="
              + targetMap.length
              + ", targetMapOffsets.length="
              + targetMapOffsets.length
              + ", sourceId="
              + sourceId);
    targetMapOffsets[sourceId] = targetMap.length;
  }

  @Deprecated(forRemoval = true, since = "9.1")
  public static final InputStream getResource(ResourceScheme scheme, String path)
      throws IOException {
    switch (scheme) {
      case CLASSPATH:
        Objects.requireNonNull(
            path,
            "Deprecated API no longer works with null paths, to load default resources use default ctors.");
        return IOUtils.requireResourceNonNull(
            BinaryDictionary.class.getClassLoader().getResourceAsStream(path), path);
      case FILE:
        return Files.newInputStream(Paths.get(path));
      default:
        throw new IllegalStateException("unknown resource scheme " + scheme);
    }
  }

  public void lookupWordIds(int sourceId, IntsRef ref) {
    ref.ints = targetMap;
    ref.offset = targetMapOffsets[sourceId];
    // targetMapOffsets always has one more entry pointing behind last:
    ref.length = targetMapOffsets[sourceId + 1] - ref.offset;
  }

  @Override
  public int getLeftId(int wordId) {
    return buffer.getShort(wordId) >>> 2;
  }

  @Override
  public int getRightId(int wordId) {
    return buffer.getShort(wordId + 2) >>> 2; // Skip left id
  }

  @Override
  public int getWordCost(int wordId) {
    return buffer.getShort(wordId + 4); // Skip left and right id
  }

  @Override
  public POS.Type getPOSType(int wordId) {
    byte value = (byte) (buffer.getShort(wordId) & 3);
    return POS.resolveType(value);
  }

  @Override
  public POS.Tag getLeftPOS(int wordId) {
    return posDict[getLeftId(wordId)];
  }

  @Override
  public POS.Tag getRightPOS(int wordId) {
    POS.Type type = getPOSType(wordId);
    if (type == POS.Type.MORPHEME || type == POS.Type.COMPOUND || hasSinglePOS(wordId)) {
      return getLeftPOS(wordId);
    } else {
      byte value = buffer.get(wordId + 6);
      return POS.resolveTag(value);
    }
  }

  @Override
  public String getReading(int wordId) {
    if (hasReadingData(wordId)) {
      int offset = wordId + 6;
      return readString(offset);
    }
    return null;
  }

  @Override
  public Morpheme[] getMorphemes(int wordId, char[] surfaceForm, int off, int len) {
    POS.Type posType = getPOSType(wordId);
    if (posType == POS.Type.MORPHEME) {
      return null;
    }
    int offset = wordId + 6;
    boolean hasSinglePos = hasSinglePOS(wordId);
    if (hasSinglePos == false) {
      offset++; // skip rightPOS
    }
    int length = buffer.get(offset++);
    if (length == 0) {
      return null;
    }
    Morpheme[] morphemes = new Morpheme[length];
    int surfaceOffset = 0;
    final POS.Tag leftPOS = getLeftPOS(wordId);
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
