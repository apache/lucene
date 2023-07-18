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

/** Controls {@link LeafFieldComparator} how to skip documents */
public enum Pruning {
  /** Not allowed to skip documents. */
  NONE,
  /**
   * Allowed to skip documents that compare strictly better than the top value, or strictly worse
   * than the bottom value.
   */
  GREATER_THAN,
  /**
   * Allowed to skip documents that compare better than the top value, or worse than or equal to the
   * bottom value.
   */
  GREATER_THAN_OR_EQUAL_TO
}
