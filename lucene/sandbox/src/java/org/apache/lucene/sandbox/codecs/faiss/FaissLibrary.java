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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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
  FaissLibrary INSTANCE = lookup();

  // TODO: Use vectorized version where available
  String NAME = "faiss_c";
  String VERSION = "1.11.0";

  private static FaissLibrary lookup() {
    final MethodHandles.Lookup lookup = MethodHandles.lookup();

    final Class<?> cls;
    try {
      cls = lookup.findClass("org.apache.lucene.sandbox.codecs.faiss.FaissLibraryNativeImpl");
    } catch (ClassNotFoundException | IllegalAccessException e) {
      throw new LinkageError("FaissLibraryNativeImpl class is missing or inaccessible", e);
    }

    final MethodHandle constr;
    try {
      constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new LinkageError("FaissLibraryNativeImpl constructor is missing or inaccessible", e);
    }

    try {
      return (FaissLibrary) constr.invoke();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("Should not throw checked exceptions", t);
    }
  }

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
