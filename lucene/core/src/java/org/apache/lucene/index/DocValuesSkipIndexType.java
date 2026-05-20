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

/** Options for skip indexes on doc values. */
public enum DocValuesSkipIndexType {
  /** No skip index should be created. */
  NONE {
    @Override
    boolean isCompatibleWith(DocValuesType dvType) {
      return true;
    }
  },
  /**
   * Record range of values. This is suitable for {@link DocValuesType#NUMERIC}, {@link
   * DocValuesType#SORTED_NUMERIC}, {@link DocValuesType#SORTED} and {@link
   * DocValuesType#SORTED_SET} doc values, and will record the min/max values per range of doc IDs.
   */
  RANGE {
    @Override
    boolean isCompatibleWith(DocValuesType dvType) {
      return dvType == DocValuesType.NUMERIC
          || dvType == DocValuesType.SORTED_NUMERIC
          || dvType == DocValuesType.SORTED
          || dvType == DocValuesType.SORTED_SET;
    }
  };

  // TODO: add support for pre-aggregated integer/float/double

  abstract boolean isCompatibleWith(DocValuesType dvType);
}
