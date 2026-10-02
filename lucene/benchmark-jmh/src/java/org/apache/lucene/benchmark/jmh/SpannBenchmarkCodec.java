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
package org.apache.lucene.benchmark.jmh;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.spann.Lucene99SpannVectorsFormat;

public class SpannBenchmarkCodec extends Lucene104Codec {

  // Default configuration for benchmark
  private final int nProbe;
  private final int numPartitions;
  private final int clusteringSample;
  private final int replicationFactor;

  public SpannBenchmarkCodec() {
    this(10, 100, 16384, 1);
  }

  public SpannBenchmarkCodec(
      int nProbe, int numPartitions, int clusteringSample, int replicationFactor) {
    super();
    this.nProbe = nProbe;
    this.numPartitions = numPartitions;
    this.clusteringSample = clusteringSample;
    this.replicationFactor = replicationFactor;
  }

  @Override
  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
    return new Lucene99SpannVectorsFormat(
        nProbe, numPartitions, clusteringSample, replicationFactor);
  }

  @Override
  public String toString() {
    return "SpannBenchmarkCodec(nProbe=" + nProbe + ", rf=" + replicationFactor + ")";
  }
}
