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
package org.apache.lucene.util.fst;

import java.io.IOException;
import org.apache.lucene.store.DataInput;

/** A type of {@link FSTReader} which needs data to be initialized before use */
public interface FSTStore extends FSTReader {

  /**
   * Initialize the FSTStore
   *
   * @param in the DataInput to read from
   * @param numBytes the number of bytes to read
   * @return this FSTStore
   * @throws IOException if exception occurred during reading the DataInput
   */
  FSTStore init(DataInput in, long numBytes) throws IOException;
}
