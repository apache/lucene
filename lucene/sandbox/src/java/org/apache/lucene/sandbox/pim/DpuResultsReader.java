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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Abstract base class for reading DPU results. This is the object returned by the DpuSystemExecutor
 * and the DpuSystemSimulator. Results are not necessarily sorted by docId. However it is possible
 * to specify a max docId and this will iterate over all docIds lower than the max. Then if the max
 * docId is increased, this will iterate over the next docIds until the new max, etc.
 */
public abstract class DpuResultsReader {

  protected final PimQuery query;
  protected PimMatch match;
  protected int baseDoc;
  protected Similarity.SimScorer simScorer;

  DpuResultsReader(PimQuery query) {
    this.query = query;
    this.match = new PimMatch(-1, 0.0F);
    this.baseDoc = 0;
    this.simScorer = null;
  }

  public void setSimScorer(Similarity.SimScorer scorer) {
    this.simScorer = scorer;
  }

  /**
   * Set the base docId. All docIds returned will be relative to the base. This can be changed when
   * reading a different segment with a different base docId.
   *
   * @param baseDoc the base docId
   */
  public void setBaseDoc(int baseDoc) {
    this.baseDoc = baseDoc;
  }

  /**
   * Go to the next result
   *
   * @return true if there is a next result to be read
   */
  public abstract boolean next() throws IOException;

  /**
   * Set the lucene segment which results need to be read
   *
   * @param segmentId the lucene segment id
   * @param maxDocSegment the max doc id of the lucene segment
   */
  public abstract void setSegmentId(int segmentId, int maxDocSegment) throws IOException;

  /**
   * Read the current result
   *
   * @return the current result as a PimMatch object
   */
  public PimMatch match() {
    return match;
  }
}
