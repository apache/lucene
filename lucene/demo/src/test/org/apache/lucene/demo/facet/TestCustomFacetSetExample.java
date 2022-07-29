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
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestCustomFacetSetExample extends LuceneTestCase {

  @Test
  public void testExactMatching() throws Exception {
    FacetResult result = new CustomFacetSetExample().runExactMatching();

    assertEquals("temperature", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(2, result.value);
    assertEquals(2, result.childCount);

    assertEquals(new LabelAndValue("May 2022 (100f)", 1), result.labelValues[0]);
    assertEquals(new LabelAndValue("July 2022 (120f)", 2), result.labelValues[1]);
  }

  @Test
  public void testRangeMatching() throws Exception {
    FacetResult result = new CustomFacetSetExample().runRangeMatching();

    assertEquals("temperature", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(2, result.value);
    assertEquals(1, result.childCount);

    assertEquals(new LabelAndValue("Eighty to Hundred Degrees", 4), result.labelValues[0]);
  }

  @Test
  public void testCustomRangeMatching() throws Exception {
    FacetResult result = new CustomFacetSetExample().runCustomRangeMatching();

    assertEquals("temperature", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(2, result.value);
    assertEquals(1, result.childCount);

    assertEquals(new LabelAndValue("Eighty to Hundred Degrees", 4), result.labelValues[0]);
  }
}
