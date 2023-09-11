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

/**
 * Class used to read results coming from PIM and score them on the fly Read the results directly
 * from the byte array filled by the PIM communication APIs, without copying
 */
public class DpuResults {

  private PimQuery query;
  private DpuResultsReader reader;
  private LeafSimScorer simScorer;
  private PimMatch match;

  DpuResults(PimQuery query, DpuResultsReader reader, LeafSimScorer simScorer) {
    this.query = query;
    this.reader = reader;
    this.simScorer = simScorer;
    this.match = new PimMatch(-1, 0.0F);
  }

  public boolean next() {
    if (reader.eof()) return false;
    try {
      query.readResult(reader, simScorer, match);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public PimMatch match() {
    return match;
  }
}
