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

package org.apache.lucene.sandbox.codecs.jvector;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;

/**
 * A custom {@link FilterCodec} that wraps the default Lucene codec with JVector vector indexing
 * support. This codec registers the {@link JVectorFormat} as the k-NN vectors format used during
 * indexing and searching.Add commentMore actions
 */
public class JVectorCodec extends FilterCodec {

  public static final String CODEC_NAME = "JVectorCodec";
  private int minBatchSizeForQuantization;
  private boolean mergeOnDisk;

  public JVectorCodec() {
    this(
        CODEC_NAME,
        new Lucene103Codec(),
        JVectorFormat.DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION,
        JVectorFormat.DEFAULT_MERGE_ON_DISK);
  }

  public JVectorCodec(int minBatchSizeForQuantization, boolean mergeOnDisk) {
    this(CODEC_NAME, new Lucene103Codec(), minBatchSizeForQuantization, mergeOnDisk);
  }

  public JVectorCodec(
      String codecName, Codec delegate, int minBatchSizeForQuantization, boolean mergeOnDisk) {
    super(codecName, delegate);
    this.minBatchSizeForQuantization = minBatchSizeForQuantization;
    this.mergeOnDisk = mergeOnDisk;
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return new JVectorFormat(minBatchSizeForQuantization, mergeOnDisk);
  }
}
