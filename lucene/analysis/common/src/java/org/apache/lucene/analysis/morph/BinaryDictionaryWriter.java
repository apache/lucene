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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;

/** Abstract base dictionary writer class. */
public abstract class BinaryDictionaryWriter<T extends BinaryDictionary<? extends MorphData>> {
  private final Class<T> implClazz;
  private int targetMapEndOffset = 0, lastWordId = -1, lastSourceId = -1;
  private int[] targetMap = new int[8192];
  private int[] targetMapOffsets = new int[8192];
  protected final DictionaryEntryWriter entryWriter;

  protected BinaryDictionaryWriter(Class<T> implClazz, DictionaryEntryWriter entryWriter) {
    this.implClazz = implClazz;
    this.entryWriter = entryWriter;
  }

  /**
   * put the entry in map
   *
   * @return current position of buffer, which will be wordId of next entry
   */
  public int put(String[] entry) {
    return entryWriter.putEntry(entry);
  }

  /**
   * Write whole dictionary in a directory.
   *
   * @throws IOException if an I/O error occurs writing the dictionary files
   */
  public abstract void write(Path baseDir) throws IOException;

  protected void addMapping(int sourceId, int wordId) {
    if (wordId <= lastWordId) {
      throw new IllegalStateException(
          "words out of order: " + wordId + " vs lastID: " + lastWordId);
    }

    if (sourceId > lastSourceId) {
      targetMapOffsets = ArrayUtil.grow(targetMapOffsets, sourceId + 1);
      for (int i = lastSourceId + 1; i <= sourceId; i++) {
        targetMapOffsets[i] = targetMapEndOffset;
      }
    } else if (sourceId != lastSourceId) {
      throw new IllegalStateException(
          "source ids not in increasing order: lastSourceId="
              + lastSourceId
              + " vs sourceId="
              + sourceId);
    }

    targetMap = ArrayUtil.grow(targetMap, targetMapEndOffset + 1);
    targetMap[targetMapEndOffset] = wordId;
    targetMapEndOffset++;

    lastSourceId = sourceId;
    lastWordId = wordId;
  }

  /**
   * Write dictionary in file Dictionary format is: [Size of dictionary(int)], [entry:{left
   * id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}],
   * [entry...], [entry...].....
   *
   * @throws IOException if an I/O error occurs writing the dictionary files
   */
  protected void write(
      Path baseDir,
      String targetMapCodecHeader,
      String posDictCodecHeader,
      String dictCodecHeader,
      int dictCodecVersion)
      throws IOException {
    final String baseName = getBaseFileName();
    entryWriter.writeDictionary(
        baseDir.resolve(baseName + BinaryDictionary.DICT_FILENAME_SUFFIX),
        dictCodecHeader,
        dictCodecVersion);
    entryWriter.writePosDict(
        baseDir.resolve(baseName + BinaryDictionary.POSDICT_FILENAME_SUFFIX),
        posDictCodecHeader,
        dictCodecVersion);
    writeTargetMap(
        baseDir.resolve(baseName + BinaryDictionary.TARGETMAP_FILENAME_SUFFIX),
        targetMapCodecHeader,
        dictCodecVersion);
  }

  protected final String getBaseFileName() {
    return implClazz.getName().replace('.', '/');
  }

  // TODO: maybe this int[] should instead be the output to the FST...
  private void writeTargetMap(Path path, String targetMapCodecHeader, int dictCodecVersion)
      throws IOException {
    Files.createDirectories(path.getParent());
    try (OutputStream os = Files.newOutputStream(path);
        OutputStream bos = new BufferedOutputStream(os)) {
      final DataOutput out = new OutputStreamDataOutput(bos);
      CodecUtil.writeHeader(out, targetMapCodecHeader, dictCodecVersion);

      final int numSourceIds = lastSourceId + 1;
      out.writeVInt(targetMapEndOffset); // <-- size of main array
      out.writeVInt(numSourceIds + 1); // <-- size of offset array (+ 1 more entry)
      int prev = 0, sourceId = 0;
      for (int ofs = 0; ofs < targetMapEndOffset; ofs++) {
        final int val = targetMap[ofs], delta = val - prev;
        assert delta >= 0;
        if (ofs == targetMapOffsets[sourceId]) {
          out.writeVInt((delta << 1) | 0x01);
          sourceId++;
        } else {
          out.writeVInt((delta << 1));
        }
        prev += delta;
      }
      if (sourceId != numSourceIds) {
        throw new IllegalStateException(
            "sourceId:" + sourceId + " != numSourceIds:" + numSourceIds);
      }
    }
  }
}
