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

package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.util.Accountable;

/**
 * Vectors' writer for a field
 *
 * @param <T> an array type; the type of vectors to be written
 */
public abstract class KnnFieldVectorsWriter<T> implements Accountable {

  /** Sole constructor */
  protected KnnFieldVectorsWriter() {}

  /**
   * Add new docID with its vector value to the given field for indexing. Doc IDs must be added in
   * increasing order.
   */
  public abstract void addValue(int docID, T vectorValue) throws IOException;

  /**
   * Used to copy values being indexed to internal storage.
   *
   * @param vectorValue an array containing the vector value to add
   * @return a copy of the value; a new array
   */
  public abstract T copyValue(T vectorValue);
}
