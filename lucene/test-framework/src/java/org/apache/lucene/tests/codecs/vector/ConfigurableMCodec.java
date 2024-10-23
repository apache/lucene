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
package org.apache.lucene.tests.codecs.vector;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.tests.util.TestUtil;

/**
 * This codec allows customization of the number of connections made for an hnsw index. Increasing
 * the number of connections can decrease the time of certain tests while still achieving the same
 * test coverage.
 */
public class ConfigurableMCodec extends FilterCodec {

  private final KnnVectorsFormat knnVectorsFormat;

  public ConfigurableMCodec() {
    super("ConfigurableMCodec", TestUtil.getDefaultCodec());
    knnVectorsFormat = new Lucene99HnswVectorsFormat(128, 100);
  }

  public ConfigurableMCodec(int maxConn) {
    super("ConfigurableMCodec", TestUtil.getDefaultCodec());
    knnVectorsFormat = new Lucene99HnswVectorsFormat(maxConn, 100);
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return knnVectorsFormat;
  }
}
