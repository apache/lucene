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
package org.apache.lucene.sandbox.codecs.faiss;

import java.io.Closeable;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.IntToIntFunction;

/**
 * Minimal interface to create and query Faiss indexes.
 *
 * @lucene.experimental
 */
interface FaissLibrary {
  FaissLibrary INSTANCE = new FaissLibraryNativeImpl();

  // TODO: Use SIMD version at runtime. The "faiss_c" library is linked to the main "faiss" library,
  //  which does not use SIMD instructions. However, there are SIMD versions of "faiss" (like
  //  "faiss_avx2", "faiss_avx512", "faiss_sve", etc.) available, which can be used by changing the
  //  dependencies of "faiss_c" using the "patchelf" utility. Figure out how to do this dynamically,
  //  or via modifications to upstream Faiss.
  String NAME = "faiss_c";
  String VERSION = "1.11.0";

  interface Index extends Closeable {
    void search(float[] query, KnnCollector knnCollector, Bits acceptDocs);

    void write(IndexOutput output);
  }

  Index createIndex(
      String description,
      String indexParams,
      VectorSimilarityFunction function,
      FloatVectorValues floatVectorValues,
      IntToIntFunction oldToNewDocId);

  Index readIndex(IndexInput input);
}
