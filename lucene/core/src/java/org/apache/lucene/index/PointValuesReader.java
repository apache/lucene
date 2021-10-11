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

/**
 * Access to numeric values.
 *
 * @lucene.experimental
 */
public interface PointValuesReader {

  /**
   * Provides access to the points in this reader.
   *
   * @lucene.experimental
   */
  @FunctionalInterface
  interface DocValueVisitor {
    /**
     * Called for all documents in the reader when called in {@link
     * #visitDocValues(DocValueVisitor)}.
     */
    void visit(int docID, byte[] packedValue) throws IOException;
  }

  /** Visit all doc and values in this reader */
  void visitDocValues(DocValueVisitor visitor) throws IOException;

  /** Returns the total number of points in this reader. */
  long size();
}
