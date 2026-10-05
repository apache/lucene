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
package org.apache.lucene.index;

import java.util.function.Function;

/** Structure of criteria on the basis of which group af a segment is selected. */
public class DWPTGroupingCriteriaDefinition {
  /** Grouping function which determines the DWPT group using which documents will be indexed. */
  private final Function<Iterable<? extends Iterable<? extends IndexableField>>, Integer>
      dwptGroupingCriteriaFunction;

  /**
   * Maximum limit on DWPT group size. Any document evaluated in a group number greater this limit
   * is indexed using default DWPT group
   */
  private final int maxDWPTGroupSize;

  /**
   * Group number of default DWPT. This group is returned for documents whose grouping function
   * outcome is greater than max group limit.
   */
  public static final int DEFAULT_DWPT_GROUP_NUMBER = -1;

  /** Grouping criteria function to select the DWPT pool group of a document. */
  public static final DWPTGroupingCriteriaDefinition DEFAULT_DWPT_GROUPING_CRITERIA_DEFINITION =
      new DWPTGroupingCriteriaDefinition(
          (document) -> {
            return DEFAULT_DWPT_GROUP_NUMBER;
          },
          1);

  /**
   * Constructor to create a DWPTGroupingCriteriaDefinition on the basis of a criteria function and
   * a max DWPT group size.
   *
   * @param dwptGroupingCriteriaFunction the grouping criteria function.
   * @param maxDWPTGroupSize maximum number of groups allowed by grouping criteria function.
   */
  public DWPTGroupingCriteriaDefinition(
      final Function<Iterable<? extends Iterable<? extends IndexableField>>, Integer>
          dwptGroupingCriteriaFunction,
      final int maxDWPTGroupSize) {
    this.dwptGroupingCriteriaFunction = dwptGroupingCriteriaFunction;
    this.maxDWPTGroupSize = maxDWPTGroupSize;
  }

  /**
   * Returns the grouping criteria function.
   *
   * @return the grouping criteria function.
   */
  public Function<Iterable<? extends Iterable<? extends IndexableField>>, Integer>
      getDwptGroupingCriteriaFunction() {
    return dwptGroupingCriteriaFunction;
  }

  /**
   * Returns the max number of groups allowed for this grouping criteria function.
   *
   * @return the max number of groups allowed for this grouping criteria function.
   */
  public int getMaxDWPTGroupSize() {
    return maxDWPTGroupSize;
  }
}
