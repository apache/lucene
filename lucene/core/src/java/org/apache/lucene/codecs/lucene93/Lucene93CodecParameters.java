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
package org.apache.lucene.codecs.lucene93;

import java.util.Objects;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;

/** Parameters for Lucene codecs * */
public class Lucene93CodecParameters {

  private StoredFieldsCompressionMode storedFieldsCompressionMode;
  private int hnswMaxConn;
  private int hnswBeamWidth;

  /** Configuration option for the codec. */
  public enum StoredFieldsCompressionMode {
    /** Trade compression ratio for retrieval speed. */
    BEST_SPEED(Lucene90StoredFieldsFormat.Mode.BEST_SPEED),
    /** Trade retrieval speed for compression ratio. */
    BEST_COMPRESSION(Lucene90StoredFieldsFormat.Mode.BEST_COMPRESSION);

    private final Lucene90StoredFieldsFormat.Mode storedMode;

    StoredFieldsCompressionMode(Lucene90StoredFieldsFormat.Mode storedMode) {
      this.storedMode = Objects.requireNonNull(storedMode);
    }

    public Lucene90StoredFieldsFormat.Mode getStoredMode() {
      return storedMode;
    }
  }

  public Lucene93CodecParameters(
      StoredFieldsCompressionMode storedFieldsCompressionMode, int hnswMaxConn, int hnswBeamWidth) {
    this.storedFieldsCompressionMode = storedFieldsCompressionMode;
    this.hnswMaxConn = hnswMaxConn;
    this.hnswBeamWidth = hnswBeamWidth;
  }

  public int getHnswMaxConn() {
    return hnswMaxConn;
  }

  public int getHnswBeamWidth() {
    return hnswBeamWidth;
  }

  public StoredFieldsCompressionMode getStoredFieldsCompressionMode() {
    return storedFieldsCompressionMode;
  }

  public static Lucene93CodecParametersBuilder builder() {
    return new Lucene93CodecParametersBuilder();
  }

  /** Builder for Lucene93CodecParameters */
  public static class Lucene93CodecParametersBuilder {
    private StoredFieldsCompressionMode storedFieldsCompressionMode =
        StoredFieldsCompressionMode.BEST_SPEED;
    private int hnswMaxConn = Lucene93HnswVectorsFormat.DEFAULT_MAX_CONN;
    private int hnswBeamWidth = Lucene93HnswVectorsFormat.DEFAULT_BEAM_WIDTH;

    public Lucene93CodecParametersBuilder withStoredFieldsCompressionMode(
        StoredFieldsCompressionMode storedFieldsCompressionMode) {
      this.storedFieldsCompressionMode = storedFieldsCompressionMode;
      return this;
    }

    public Lucene93CodecParametersBuilder withHnswBeamWidth(int hnswBeamWidth) {
      this.hnswBeamWidth = hnswBeamWidth;
      return this;
    }

    public Lucene93CodecParametersBuilder withHnswMaxConn(int hnswMaxConn) {
      this.hnswMaxConn = hnswMaxConn;
      return this;
    }

    public Lucene93CodecParameters build() {
      return new Lucene93CodecParameters(
          this.storedFieldsCompressionMode, this.hnswMaxConn, this.hnswBeamWidth);
    }
  }
}
