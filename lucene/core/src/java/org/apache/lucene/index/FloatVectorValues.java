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

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.logging.Logger;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.SuppressForbidden;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * KnnFloatVectorField}.
 *
 * @lucene.experimental
 */
public abstract class FloatVectorValues extends DocIdSetIterator {
  private static final Logger LOG = Logger.getLogger(FloatVectorValues.class.getName());

  /**
   * The maximum length of a vector. Can be overridden via a system property
   * "org.apache.lucene.hnsw.maxDimensions".
   *
   * @deprecated Expected to move to a codec specific limit.
   */
  @Deprecated public static final int MAX_DIMENSIONS = doPrivileged(FloatVectorValues::initMaxDim);

  // Extracted to a method to be able to apply the SuppressForbidden annotation
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static <T> T doPrivileged(PrivilegedAction<T> action) {
    return AccessController.doPrivileged(action);
  }

  private static int initMaxDim() {
    final var PROP_NAME = "org.apache.lucene.hnsw.maxDimensions";
    final int DEFAULT_MAX = 1024;
    try {
      int d = Integer.getInteger(PROP_NAME, DEFAULT_MAX);
      if (d > 2048) {
        LOG.warning("Lucene HNSW only supports up to 2048 dimensions.");
      }
      return d;
    } catch (
        @SuppressWarnings("unused")
        Exception ignored) { // e.g. SecurityException
      LOG.warning(
          "Cannot read sysprop " + PROP_NAME + ", so the dimension limit will be unchanged.");
      return DEFAULT_MAX;
    }
  }

  /** Sole constructor */
  protected FloatVectorValues() {}

  /** Return the dimension of the vectors */
  public abstract int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  @Override
  public final long cost() {
    return size();
  }

  /**
   * Return the vector value for the current document ID. It is illegal to call this method when the
   * iterator is not positioned: before advancing, or after failing to advance. The returned array
   * may be shared across calls, re-used, and modified as the iterator advances.
   *
   * @return the vector value
   */
  public abstract float[] vectorValue() throws IOException;
}
