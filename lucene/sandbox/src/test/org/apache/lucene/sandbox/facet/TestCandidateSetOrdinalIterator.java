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

package org.apache.lucene.sandbox.facet;

import java.io.IOException;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.sandbox.facet.iterators.CandidateSetOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.LabelToOrd;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LeafFacetRecorder;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for {@link CandidateSetOrdinalIterator}. */
public class TestCandidateSetOrdinalIterator extends LuceneTestCase {

  /** LabelToOrd that parses label's string to get int ordinal */
  private LabelToOrd mockLabelToOrd =
      new LabelToOrd() {
        @Override
        public int getOrd(FacetLabel label) {
          return Integer.valueOf(label.lastComponent());
        }

        @Override
        public int[] getOrds(FacetLabel[] labels) {
          int[] result = new int[labels.length];
          for (int i = 0; i < result.length; i++) {
            result[i] = getOrd(labels[i]);
          }
          return result;
        }
      };

  private FacetCutter mockFacetCutter =
      new FacetCutter() {
        @Override
        public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
          return null;
        }
      };

  public void testBasic() throws IOException {
    FacetRecorder recorder = new CountFacetRecorder();
    LeafFacetRecorder leafRecorder = recorder.getLeafRecorder(null);
    leafRecorder.record(0, 0);
    leafRecorder.record(0, 3);
    recorder.reduce(mockFacetCutter);

    FacetLabel[] candidates =
        new FacetLabel[] {
          new FacetLabel("0"),
          new FacetLabel("1"),
          new FacetLabel(String.valueOf(LabelToOrd.INVALID_ORD)),
          new FacetLabel("3")
        };

    // Note that "1" is filtered out as it was never recorded
    assertArrayEquals(
        new int[] {0, 3},
        new CandidateSetOrdinalIterator(recorder, candidates, mockLabelToOrd).toArray());
  }

  public void testEmptyRecorder() throws IOException {
    FacetRecorder recorder = new CountFacetRecorder();
    recorder.reduce(mockFacetCutter);

    FacetLabel[] candidates =
        new FacetLabel[] {
          new FacetLabel("0"),
          new FacetLabel("1"),
          new FacetLabel(String.valueOf(LabelToOrd.INVALID_ORD)),
          new FacetLabel("3")
        };

    // Note that "1" is filtered out as it was never recorded
    assertEquals(
        OrdinalIterator.NO_MORE_ORDS,
        new CandidateSetOrdinalIterator(recorder, candidates, mockLabelToOrd).nextOrd());
  }
}
