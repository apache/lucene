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
package org.apache.lucene.codecs.zstd;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;

/** Codec that uses ZSTD for stored fields. */
public final class Zstd90Codec extends FilterCodec {

  private final StoredFieldsFormat format;
  private final int level;

  /** Default constructor. */
  public Zstd90Codec() {
    this(10);
  }

  /** Create a new codec using the configured compression level. */
  public Zstd90Codec(int level) {
    super("Zstd90", new Lucene90Codec());
    this.format = new ZstdStoredFieldsFormat(level);
    this.level = level;
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return format;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(level=" + level + ")";
  }
}
