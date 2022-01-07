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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** Stored field format used by plugaable codec */
public class Lucene90CustomCompressionStoredFieldsFormat extends StoredFieldsFormat {
  // chunk size for zstandard
  private static final int ZSTD_BLOCK_LENGTH = 10 * 48 * 1024;

  public static final String MODE_KEY =
      Lucene90CustomCompressionStoredFieldsFormat.class.getSimpleName() + ".mode";

  final Lucene90CustomCompressionCodec.Mode mode;

  private int compressionLevel;

  /** default constructor */
  public Lucene90CustomCompressionStoredFieldsFormat() {
    this(
        Lucene90CustomCompressionCodec.Mode.ZSTD_DICT,
        Lucene90CustomCompressionCodec.defaultCompressionLevel);
  }

  /** Stored fields format with specified compression algo. */
  public Lucene90CustomCompressionStoredFieldsFormat(
      Lucene90CustomCompressionCodec.Mode mode, int compressionLevel) {
    this.mode = Objects.requireNonNull(mode);
    this.compressionLevel = compressionLevel;
  }

  @Override
  public StoredFieldsReader fieldsReader(
      Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
    String value = si.getAttribute(MODE_KEY);
    if (value == null) {
      throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
    }
    Lucene90CustomCompressionCodec.Mode mode = Lucene90CustomCompressionCodec.Mode.valueOf(value);
    return impl(mode).fieldsReader(directory, si, fn, context);
  }

  @Override
  public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context)
      throws IOException {
    String previous = si.putAttribute(MODE_KEY, mode.name());
    if (previous != null && previous.equals(mode.name()) == false) {
      throw new IllegalStateException(
          "found existing value for "
              + MODE_KEY
              + " for segment: "
              + si.name
              + "old="
              + previous
              + ", new="
              + mode.name());
    }
    return impl(mode).fieldsWriter(directory, si, context);
  }

  StoredFieldsFormat impl(Lucene90CustomCompressionCodec.Mode mode) {
    switch (mode) {
      case ZSTD:
        return new Lucene90CompressingStoredFieldsFormat(
            "CustomCompressionStoredFieldsZSTD", ZSTD_MODE_NO_DICT, ZSTD_BLOCK_LENGTH, 4096, 10);

      case ZSTD_DICT:
        return new Lucene90CompressingStoredFieldsFormat(
            "CustomCompressionStoredFieldsZSTD", ZSTD_MODE_DICT, ZSTD_BLOCK_LENGTH, 4096, 10);
      default:
        throw new AssertionError();
    }
  }

  public final CompressionMode ZSTD_MODE_NO_DICT = new ZstdCompressionMode(compressionLevel);
  public final CompressionMode ZSTD_MODE_DICT = new ZstdDictCompressionMode(compressionLevel);
}
