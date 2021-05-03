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
package org.apache.lucene.backward_codecs.packed;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Legacy PackedInts operations.
 *
 * @lucene.internal
 */
public class LegacyPackedInts {

  private LegacyPackedInts() {
    // no instances
  }

  /**
   * Expert: Restore a {@code PackedInts.Reader} from a stream without reading metadata at the
   * beginning of the stream. This method is useful to restore data from streams which have been
   * created using {@link PackedInts#getWriterNoHeader(DataOutput, PackedInts.Format, int, int,
   * int)}.
   *
   * @param in the stream to read data from, positioned at the beginning of the packed values
   * @param format the format used to serialize
   * @param version the version used to serialize the data
   * @param valueCount how many values the stream holds
   * @param bitsPerValue the number of bits per value
   * @return a Reader
   * @throws IOException If there is a low-level I/O error
   * @see PackedInts#getWriterNoHeader(DataOutput, PackedInts.Format, int, int, int)
   * @lucene.internal
   */
  public static PackedInts.Reader getReaderNoHeader(
      DataInput in, PackedInts.Format format, int version, int valueCount, int bitsPerValue)
      throws IOException {
    PackedInts.checkVersion(version);
    switch (format) {
      case PACKED_SINGLE_BLOCK:
        return LegacyPacked64SingleBlock.create(in, valueCount, bitsPerValue);
      case PACKED:
        return new LegacyPacked64(version, in, valueCount, bitsPerValue);
      default:
        throw new AssertionError("Unknown Writer format: " + format);
    }
  }
}
