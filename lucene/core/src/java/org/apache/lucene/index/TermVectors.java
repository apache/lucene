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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute; // javadocs

/**
 * API for reading term vectors.
 *
 * <p><b>NOTE</b>: This class is not thread-safe and should only be consumed in the thread where it
 * was acquired.
 */
public abstract class TermVectors {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected TermVectors() {}

  /**
   * Returns term vectors for this document, or null if term vectors were not indexed.
   *
   * <p>The returned Fields instance acts like a single-document inverted index (the docID will be
   * 0). If offsets are available they are in an {@link OffsetAttribute} available from the {@link
   * PostingsEnum}.
   */
  public abstract Fields get(int doc) throws IOException;

  /**
   * Retrieve term vector for this document and field, or null if term vectors were not indexed.
   *
   * <p>The returned Terms instance acts like a single-document inverted index (the docID will be
   * 0). If offsets are available they are in an {@link OffsetAttribute} available from the {@link
   * PostingsEnum}.
   */
  public final Terms get(int doc, String field) throws IOException {
    Fields vectors = get(doc);
    if (vectors == null) {
      return null;
    }
    return vectors.terms(field);
  }

  /** Instance that never returns term vectors */
  public static final TermVectors EMPTY =
      new TermVectors() {
        @Override
        public Fields get(int doc) {
          return null;
        }
      };
}
