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
package org.apache.lucene.codecs.customcompression;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;

/** Custom codec for different compression algorithm */
public final class Lucene90CustomCompressionCodec extends FilterCodec {

  private final StoredFieldsFormat storedFieldsFormat;
  private int compressionLevel;
  public static final int defaultCompressionLevel = 3;

  /** Compression modes */
  public static enum Mode {

    // Currently Zstandard is supported, other compression algorithms can be implemented and
    // respective modes can be added
    // for e.g. ZSTD, ZSTD_DICT, ZSTD_FAST or any other compression algos
    ZSTD_COMPRESSION
  }
  /** Default codec */
  public Lucene90CustomCompressionCodec() {
    this(Mode.ZSTD_COMPRESSION, defaultCompressionLevel);
  }

  /** new codec for a given compression algorithm and compression level */
  public Lucene90CustomCompressionCodec(Mode compressionMode, int compressionLevel) {
    super("Lucene90CustomCompression", new Lucene90Codec());
    this.compressionLevel = compressionLevel;

    switch (compressionMode) {
      case ZSTD_COMPRESSION:
        if (this.compressionLevel < 1 || this.compressionLevel > 22)
          throw new IllegalArgumentException("Invalid compression level");

        this.storedFieldsFormat =
            new Lucene90CustomCompressionStoredFieldsFormat(
                Mode.ZSTD_COMPRESSION, compressionLevel);
        break;

      default:
        throw new IllegalArgumentException("Chosen compression mode does not exist");
    }
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
