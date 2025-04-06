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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.BinScoreLeafReader;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

/** Computes per-bin score boosts using inverse frequency heuristics. */
final class BinBoostCalculator {

  private BinBoostCalculator() {}

  /** Computes bin-level boosts using inverse frequency weighting with log scaling. */
  static float[] compute(IndexReader reader) throws IOException {
    int[] binCounts = new int[8]; // start small, grow as needed
    int totalDocs = 0;
    int maxBin = -1;

    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      BinScoreReader binReader =
          (leaf instanceof BinScoreLeafReader)
              ? ((BinScoreLeafReader) leaf).getBinScoreReader()
              : null;
      if (binReader == null) {
        continue;
      }

      for (int docID = 0; docID < leaf.maxDoc(); docID++) {
        int bin = binReader.getBinForDoc(docID);
        if (bin >= binCounts.length) {
          binCounts = Arrays.copyOf(binCounts, Math.max(bin + 1, binCounts.length * 2));
        }
        binCounts[bin]++;
        totalDocs++;
        maxBin = Math.max(maxBin, bin);
      }
    }

    if (totalDocs == 0 || maxBin < 0) {
      return new float[0];
    }

    float[] boosts = new float[maxBin + 1];
    float maxBoost = 0f;

    for (int bin = 0; bin <= maxBin; bin++) {
      int count = binCounts[bin];
      if (count == 0) {
        boosts[bin] = 0f;
        continue;
      }

      // Boost is proportional to inverse frequency
      float freq = (float) count / totalDocs;
      float boost = (float) Math.log((1.0f + 1.0f / freq)); // avoid log(0)
      boosts[bin] = boost;
      maxBoost = Math.max(maxBoost, boost);
    }

    // Normalize boosts to the range [1.0, max], where 1.0 is neutral
    if (maxBoost > 0f) {
      for (int i = 0; i < boosts.length; i++) {
        if (boosts[i] == 0f) {
          continue;
        }
        // Scale so the most underrepresented bin gets higher score
        boosts[i] = 1.0f + (boosts[i] / maxBoost); // final boost ∈ [1.0, 2.0]
      }
    }

    return boosts;
  }
}
