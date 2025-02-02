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
package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.LibraryException;
import java.util.logging.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;

/** CuVS based codec for GPU based vector search */
public class CuVSCodec extends FilterCodec {

  public CuVSCodec() {
    this("CuVSCodec", new Lucene101Codec());
  }

  public CuVSCodec(String name, Codec delegate) {
    super(name, delegate);
    KnnVectorsFormat format;
    try {
      format = new CuVSVectorsFormat(1, 128, 64, MergeStrategy.NON_TRIVIAL_MERGE);
      setKnnFormat(format);
    } catch (LibraryException ex) {
      Logger log = Logger.getLogger(CuVSCodec.class.getName());
      log.severe("Couldn't load native library, possible classloader issue. " + ex.getMessage());
    }
  }

  KnnVectorsFormat knnFormat = null;

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return knnFormat;
  }

  public void setKnnFormat(KnnVectorsFormat format) {
    this.knnFormat = format;
  }
}
