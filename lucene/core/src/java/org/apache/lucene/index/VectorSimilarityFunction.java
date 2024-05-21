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
package org.apache.lucene.index;

import org.apache.lucene.util.NamedSPILoader;

/**
 * Vector similarity function; used in search to return top K most similar vectors to a target
 * vector. This is a label describing the method used during indexing and searching of the vectors
 * in order to determine the nearest neighbors.
 */
public abstract class VectorSimilarityFunction implements NamedSPILoader.NamedSPI {

  private static class Holder {
    private static final NamedSPILoader<VectorSimilarityFunction> LOADER =
        new NamedSPILoader<>(VectorSimilarityFunction.class);

    static NamedSPILoader<VectorSimilarityFunction> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a SortFieldProvider by name before all SortFieldProviders could be initialized. "
                + "This likely happens if you call SortFieldProvider#forName from a SortFieldProviders's ctor.");
      }
      return LOADER;
    }
  }

  private final String name;
  private final int ordinal;

  /** Construct onbject with function name and ordinal value */
  protected VectorSimilarityFunction(String name, int ordinal) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
    this.ordinal = ordinal;
  }

  /** Get name of VectorSimilarityFunction used by the object */
  @Override
  public String getName() {
    return name;
  }

  /** Get ordinal of VectorSimilarityFunction used by the object */
  public int getOrdinal() {
    return ordinal;
  }

  /** Compares two float vector */
  public abstract float compare(float[] v1, float[] v2);

  /** Compares two byte vector */
  public abstract float compare(byte[] v1, byte[] v2);

  /** look up for VectorSimilarityFunction using name */
  public static VectorSimilarityFunction forName(String name) {
    return Holder.getLoader().lookup(name);
  }
}
