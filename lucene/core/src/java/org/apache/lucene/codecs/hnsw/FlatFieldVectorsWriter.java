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

import org.apache.lucene.codecs.KnnFieldVectorsWriter;

/**
 * Vectors' writer for a field
 *
 * @param <T> an array type; the type of vectors to be written
 * @lucene.experimental
 */
public abstract class FlatFieldVectorsWriter<T> extends KnnFieldVectorsWriter<T> {

  /**
   * The delegate to write to, can be null When non-null, all vectors seen should be written to the
   * delegate along with being written to the flat vectors.
   */
  protected final KnnFieldVectorsWriter<T> indexingDelegate;

  /**
   * Sole constructor that expects some indexingDelegate. All vectors seen should be written to the
   * delegate along with being written to the flat vectors.
   *
   * @param indexingDelegate the delegate to write to, can be null
   */
  protected FlatFieldVectorsWriter(KnnFieldVectorsWriter<T> indexingDelegate) {
    this.indexingDelegate = indexingDelegate;
  }
}
