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

package org.apache.lucene.codecs.hnsw;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.lucene.internal.vectorization.VectorizationProvider;

/**
 * A utility class that provides access to the default FlatVectorsScorer.
 *
 * @lucene.experimental
 */
public class FlatVectorScorerProvider {

  /** Returns the default FlatVectorsScorer. */
  public static FlatVectorsScorer createDefault() {
    if (isPanamaVectorUtilSupportEnabled()) {
      // we only enable this scorer if the Panama vector provider is also enabled
      return lookup();
    }
    return new DefaultFlatVectorScorer();
  }

  public static FlatVectorsScorer lookup() {
    try {
      var cls =
          Class.forName("org.apache.lucene.internal.vectorization.MemorySegmentFlatVectorsScorer");
      var lookup = MethodHandles.lookup();
      var mh =
          lookup.findConstructor(cls, MethodType.methodType(void.class, FlatVectorsScorer.class));
      return (FlatVectorsScorer) mh.invoke(new DefaultFlatVectorScorer());
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  private static boolean isPanamaVectorUtilSupportEnabled() {
    var name = VectorizationProvider.getInstance().getClass().getSimpleName();
    assert assertExpectedProvider(name);
    return name.equals("PanamaVectorizationProvider");
  }

  static boolean assertExpectedProvider(String name) {
    return name.equals("PanamaVectorizationProvider")
        || name.equals("DefaultVectorizationProvider");
  }
}
