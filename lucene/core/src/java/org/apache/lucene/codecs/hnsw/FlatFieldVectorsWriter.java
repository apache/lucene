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

import java.io.IOException;
import java.util.List;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;

/**
 * Vectors' writer for a field
 *
 * @param <T> an array type; the type of vectors to be written
 * @lucene.experimental
 */
public abstract class FlatFieldVectorsWriter<T> extends KnnFieldVectorsWriter<T> {
  /**
   * @return a list of vectors to be written
   */
  public abstract List<T> getVectors();

  /**
   * @return the docsWithFieldSet for the field writer
   */
  public abstract DocsWithFieldSet getDocsWithFieldSet();

  /**
   * indicates that this writer is done and no new vectors are allowed to be added
   *
   * @throws IOException if an I/O error occurs
   */
  public abstract void finish() throws IOException;

  /**
   * @return true if the writer is done and no new vectors are allowed to be added
   */
  public abstract boolean isFinished();
}
