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
package org.apache.lucene.internal.vectorization;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.logging.Logger;
import jdk.incubator.vector.FloatVector;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SuppressForbidden;

/** A vectorization provider that leverages the Panama Vector API. */
final class PanamaVectorizationProvider extends VectorizationProvider {

  private final VectorUtilSupport vectorUtilSupport;

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  PanamaVectorizationProvider() {
    // hack to work around for JDK-8309727:
    try {
      doPrivileged(
          () ->
              FloatVector.fromArray(
                  FloatVector.SPECIES_PREFERRED,
                  new float[FloatVector.SPECIES_PREFERRED.length()],
                  0));
    } catch (SecurityException se) {
      throw new UnsupportedOperationException(
          "We hit initialization failure described in JDK-8309727: " + se);
    }

    if (PanamaVectorUtilSupport.VECTOR_BITSIZE < 128) {
      throw new UnsupportedOperationException(
          "Vector bit size is less than 128: " + PanamaVectorUtilSupport.VECTOR_BITSIZE);
    }

    this.vectorUtilSupport = new PanamaVectorUtilSupport();

    var log = Logger.getLogger(getClass().getName());
    log.info(
        String.format(
            Locale.ENGLISH,
            "Java vector incubator API enabled; uses preferredBitSize=%d%s%s",
            PanamaVectorUtilSupport.VECTOR_BITSIZE,
            Constants.HAS_FAST_VECTOR_FMA ? "; FMA enabled" : "",
            PanamaVectorUtilSupport.HAS_FAST_INTEGER_VECTORS
                ? ""
                : "; floating-point vectors only"));
  }

  @Override
  public VectorUtilSupport getVectorUtilSupport() {
    return vectorUtilSupport;
  }

  @Override
  public FlatVectorsScorer getLucene99FlatVectorsScorer() {
    return Lucene99MemorySegmentFlatVectorsScorer.INSTANCE;
  }
}
