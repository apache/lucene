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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IntsRef;

/** Abstract dictionary base class. */
public abstract class BinaryDictionary<T extends MorphData> implements Dictionary<T> {
  public static final String DICT_FILENAME_SUFFIX = "$buffer.dat";
  public static final String TARGETMAP_FILENAME_SUFFIX = "$targetMap.dat";
  public static final String POSDICT_FILENAME_SUFFIX = "$posDict.dat";

  private final int[] targetMapOffsets, targetMap;
  protected final ByteBuffer buffer;

  protected BinaryDictionary(
      IOSupplier<InputStream> targetMapResource,
      IOSupplier<InputStream> dictResource,
      String targetMapCodecHeader,
      String dictCodecHeader,
      int dictCodecVersion)
      throws IOException {
    try (InputStream mapIS = new BufferedInputStream(targetMapResource.get())) {
      final DataInput in = new InputStreamDataInput(mapIS);
      CodecUtil.checkHeader(in, targetMapCodecHeader, dictCodecVersion, dictCodecVersion);
      this.targetMap = new int[in.readVInt()];
      this.targetMapOffsets = new int[in.readVInt()];
      populateTargetMap(in, this.targetMap, this.targetMapOffsets);
    }

    // no buffering here, as we load in one large buffer
    try (InputStream dictIS = dictResource.get()) {
      final DataInput in = new InputStreamDataInput(dictIS);
      CodecUtil.checkHeader(in, dictCodecHeader, dictCodecVersion, dictCodecVersion);
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

  public void lookupWordIds(int sourceId, IntsRef ref) {
    ref.ints = targetMap;
    ref.offset = targetMapOffsets[sourceId];
    // targetMapOffsets always has one more entry pointing behind last:
    ref.length = targetMapOffsets[sourceId + 1] - ref.offset;
  }
}
