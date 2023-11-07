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
package org.apache.lucene.demo.facet;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestRangeFacetsExample extends LuceneTestCase {

  @Test
  public void testSimple() throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();
    FacetResult result = example.search();
    assertEquals(
        "dim=timestamp path=[] value=87 childCount=3\n  Past hour (4)\n  Past six hours (22)\n  Past day (87)\n",
        result.toString());

    result = example.searchTopChildren();
    assertEquals(
        "dim=error timestamp path=[] value=2758 childCount=163\n"
            + "  Hour 104-105 (34)\n"
            + "  Hour 139-140 (34)\n"
            + "  Hour 34-35 (34)\n"
            + "  Hour 69-70 (34)\n"
            + "  Hour 103-104 (33)\n"
            + "  Hour 138-139 (33)\n"
            + "  Hour 33-34 (33)\n"
            + "  Hour 68-69 (33)\n"
            + "  Hour 102-103 (32)\n"
            + "  Hour 137-138 (32)\n",
        result.toString());
    example.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDrillDown() throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();
    TopDocs hits = example.drillDown(example.PAST_SIX_HOURS);
    assertEquals(22, hits.totalHits.value);
    example.close();
  }
}
