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
package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.SmallFloat;

/**
 * Utils for merging impacts for SynonymQuery, CombinedFieldsQuery etc
 *
 * @lucene.internal
 */
public final class ImpactsMergingUtils {
  /** Cache of decoded norms. */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

  /**
   * Return the minimum level whose impacts are valid up to {@code docIdUpTo}, or {@code -1} if
   * there is no such level.
   */
  private static int getLevel(Impacts impacts, int docIdUpTo) {
    for (int level = 0, numLevels = impacts.numLevels(); level < numLevels; ++level) {
      if (impacts.getDocIdUpTo(level) >= docIdUpTo) {
        return level;
      }
    }
    return -1;
  }

  private static class SubIterator {
    final Iterator<Impact> iterator;
    int previousFreq;
    Impact current;

    SubIterator(Iterator<Impact> iterator) {
      this.iterator = iterator;
      this.current = iterator.next();
    }

    void next() {
      previousFreq = current.freq;
      if (iterator.hasNext() == false) {
        current = null;
      } else {
        current = iterator.next();
      }
    }
  }

  private static double normToLength(long norm) {
    return LENGTH_TABLE[Byte.toUnsignedInt((byte) norm)];
  }

  /**
   * Merge impacts from multiple impactsEnum (terms matches) within the same field. The high level
   * logic is to combine freqs that have the same norm from impacts.
   */
  public static List<Impact> mergeImpactsPerField(
      ImpactsEnum[] impactsEnum,
      Impacts[] impacts,
      float[] termBoosts,
      int docIdUpTo,
      boolean combineMultiNorms) {
    assert impactsEnum.length == impacts.length;
    assert impactsEnum.length == termBoosts.length;

    List<List<Impact>> toMerge = new ArrayList<>();

    for (int i = 0; i < impactsEnum.length; ++i) {
      if (impactsEnum[i].docID() <= docIdUpTo) {
        int impactsLevel = getLevel(impacts[i], docIdUpTo);
        if (impactsLevel == -1) {
          // One instance doesn't have impacts that cover up to docIdUpTo
          // Return impacts that trigger the maximum score
          return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
        }
        final List<Impact> impactList;
        if (termBoosts[i] != 1f) {
          float boost = termBoosts[i];
          impactList =
              impacts[i].getImpacts(impactsLevel).stream()
                  .map(
                      impact -> {
                        int boostedFreq = (int) Math.ceil(impact.freq * boost);
                        long boostedNorm =
                            combineMultiNorms
                                ? SmallFloat.intToByte4(
                                    (int) Math.floor(normToLength(impact.norm) * boost))
                                : impact.norm;
                        return new Impact(boostedFreq, boostedNorm);
                      })
                  .collect(Collectors.toList());
        } else {
          impactList = impacts[i].getImpacts(impactsLevel);
        }
        toMerge.add(impactList);
      }
    }

    // all impactEnums for this field were positioned beyond docIdUpTo, which is possible when
    // 1. there are multiple fields involved.
    // 2. docIdUpTo was taken from minimum from all impactEnums across fields
    if (toMerge.size() == 0) {
      return new ArrayList<>();
    }

    if (toMerge.size() == 1) {
      // common if one synonym is common and the other one is rare
      return toMerge.get(0);
    }

    PriorityQueue<SubIterator> pq =
        new PriorityQueue<SubIterator>(impacts.length) {
          @Override
          protected boolean lessThan(SubIterator a, SubIterator b) {
            if (a.current == null) { // means iteration is finished
              return false;
            }
            if (b.current == null) {
              return true;
            }
            return Long.compareUnsigned(a.current.norm, b.current.norm) < 0;
          }
        };
    for (List<Impact> toMergeImpacts : toMerge) {
      pq.add(new SubIterator(toMergeImpacts.iterator()));
    }

    List<Impact> mergedImpacts = new ArrayList<>();

    // Idea: merge impacts by norm. The tricky thing is that we need to
    // consider norm values that are not in the impacts too. For
    // instance if the list of impacts is [{freq=2,norm=10}, {freq=4,norm=12}],
    // there might well be a document that has a freq of 2 and a length of 11,
    // which was just not added to the list of impacts because {freq=2,norm=10}
    // is more competitive. So the way it works is that we track the sum of
    // the term freqs that we have seen so far in order to account for these
    // implicit impacts.

    long sumTf = 0;
    SubIterator top = pq.top();
    do {
      final long norm = top.current.norm;
      do {
        sumTf += top.current.freq - top.previousFreq;
        top.next();
        top = pq.updateTop();
      } while (top.current != null && top.current.norm == norm);

      final int freqUpperBound = (int) Math.min(Integer.MAX_VALUE, sumTf);
      if (mergedImpacts.isEmpty()) {
        mergedImpacts.add(new Impact(freqUpperBound, norm));
      } else {
        Impact prevImpact = mergedImpacts.get(mergedImpacts.size() - 1);
        assert Long.compareUnsigned(prevImpact.norm, norm) < 0;
        if (freqUpperBound > prevImpact.freq) {
          mergedImpacts.add(new Impact(freqUpperBound, norm));
        } // otherwise the previous impact is already more competitive
      }
    } while (top.current != null);

    return mergedImpacts;
  }
}
