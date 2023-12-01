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

/** Class to read DPU results coming from the executor (see DpuSystemExecutor) */
public class DpuExecutorResultsReader extends DpuResultsReader {

  private final DpuDataInput input;
  private int maxDoc;

  DpuExecutorResultsReader(PimQuery query, DpuDataInput input) {
    super(query);
    this.input = input;
    this.maxDoc = Integer.MAX_VALUE;
    input.beginDpu();
  }

  @Override
  public boolean next() throws IOException {

    while (true) {
      // first move to the next DPU that has results
      while (input.eof() && !input.lastDpu()) {
        input.nextDpu();
      }
      // if no more DPU results, this is the end of the
      // results for the current segment
      // reset to first DPU and return false
      if (input.eof()) {
        input.beginDpu();
        return false;
      }

      // continue moving to the next DPU until we find a
      // docId lower than the current segment max, or we reached
      // the last DPU
      int nextDocId = input.peekDocID();
      while (nextDocId >= maxDoc && !input.lastDpu()) {
        // passed the maximum doc id
        // need to go to the next DPU
        input.nextDpu();
        if (!input.eof()) nextDocId = input.peekDocID();
      }
      if (nextDocId >= maxDoc) {
        // we have finished reading the current segment
        // reset to first DPU and return
        input.beginDpu();
        return false;
      } else {
        match.docId = input.readDocId() - baseDoc;
        break;
      }
    }

    // read the result for this doc ID
    // use method specific to the query
    match.score = query.scorePimResult(input.readInt(), match.docId, simScorer);
    return true;
  }

  @Override
  public void setSegmentId(int segmentId, int maxDocSegment) throws IOException {

    if(maxDocSegment < maxDoc)
      throw new IOException("DpuExecutorResultsReader can only read segments in increasing order");
    maxDoc = maxDocSegment;
  }
}
