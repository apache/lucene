/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Objects;
import org.apache.lucene.codecs.BinMapReader;

/**
 * Provides per-document bin assignments for scoring adjustments.
 *
 * @lucene.experimental
 */
public final class BinScoreReader {

  private final BinMapReader binMap;

  /** Creates a new {@link BinScoreReader} from a {@link BinMapReader}. */
  public BinScoreReader(BinMapReader binMap) {
    this.binMap = Objects.requireNonNull(binMap, "binMap must not be null");
  }

  /**
   * Returns the bin ID for the specified document.
   *
   * @param docID doc ID within the segment
   * @return assigned bin ID
   * @throws IOException if bin lookup fails
   */
  public int getBinForDoc(int docID) throws IOException {
    return binMap.getBin(docID);
  }

  /** Returns the total number of bins in this segment. */
  public int getBinCount() {
    return binMap.getBinCount();
  }
}