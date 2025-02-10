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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

/** Utility class to decode BKD docIds. */
public class BKDDecodingUtil {

  /** Sole constructor, called by sub-classes. */
  BKDDecodingUtil() {}

  public void decodeDelta16(IndexInput in, int[] docIds, int count) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >> 1;
    in.readInts(docIds, 0, halfLen);
    for (int i = 0; i < halfLen; ++i) {
      int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  public void decode24(IndexInput in, int[] docIds, int[] scratch, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(scratch, 0, quarterLen3);
    for (int i = 0; i < quarterLen3; ++i) {
      docIds[i] = scratch[i] >>> 8;
    }
    for (int i = 0; i < quarterLen; i++) {
      docIds[i + quarterLen3] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarterLen] & 0xFF) << 8)
              | (scratch[i + quarterLen * 2] & 0xFF);
    }
    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(docIds, quarterLen << 2, remainder);
    }
  }
}
