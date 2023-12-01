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
import java.util.List;

/** Class to read DPU results coming from the Simulator (see DpuSystemSimulator) */
public class DpuSimulatorResultsReader extends DpuResultsReader {

  private final List<PimMatch> matches;
  private int currId;
  private int maxDoc;

  DpuSimulatorResultsReader(PimQuery query, List<PimMatch> matches) {
    super(query);
    this.matches = matches;
    this.currId = 0;
    this.maxDoc = Integer.MAX_VALUE;
  }

  @Override
  public boolean next() throws IOException {
    if (currId >= matches.size()) return false;
    if (matches.get(currId).docId >= maxDoc) return false;
    match = matches.get(currId);
    match.docId -= baseDoc;
    match.score = simScorer.score(match.docId, match().score);
    currId++;
    return true;
  }

  @Override
  public void setSegmentId(int segmentId, int maxDocSegment) throws IOException {

    if(maxDocSegment < maxDoc)
      throw new IOException("DpuSimulatorResultsReader can only read segments in increasing order");
    maxDoc = maxDocSegment;
  }
}
