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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TestNegativeDisjunctMultipassPresearcher extends NegativeDisjunctPresearcherTestBase {

  @Override
  protected Presearcher createPresearcher(
      int maxNegativeDisjuncts, List<CustomQueryHandler> handlers) {
    return new MultipassTermFilteredPresearcher(
        4, 0, TermWeightor.DEFAULT, handlers, Set.of(), maxNegativeDisjuncts);
  }

  private static Presearcher createPresearcher(int passes, int maxNegativeDisjuncts) {
    return new MultipassTermFilteredPresearcher(
        passes, 0, TermWeightor.DEFAULT, List.of(), Set.of(), maxNegativeDisjuncts);
  }

  public void testNegativeDisjunctFillsAPassThePositivesLeaveUnused() throws IOException {
    // pass 0 takes x:abc, pass 1 the negative disjunct
    assertRunAndMatched(
        0, 0, run(createPresearcher(2, 1), "+x:abc -f:t", doc("x", "abc", "f", "t")));
    assertRunAndMatched(1, 1, run(createPresearcher(2, 1), "+x:abc -f:t", doc("x", "abc")));
    // with one pass the negative disjunct is never reached
    assertRunAndMatched(
        1, 0, run(createPresearcher(1, 1), "+x:abc -f:t", doc("x", "abc", "f", "t")));
  }

  public void testOverflowingPassStillLetsOtherPassesFilter() throws IOException {
    String query = "+x:abc -(+y:b +z:c)";
    // pass 0 takes x:abc; pass 1 has two negative disjuncts for a budget of one, so it accepts
    // everything
    assertRunAndMatched(0, 0, run(createPresearcher(2, 1), query, doc("y", "b", "z", "c")));
    assertRunAndMatched(
        1, 0, run(createPresearcher(2, 1), query, doc("x", "abc", "y", "b", "z", "c")));
  }
}
