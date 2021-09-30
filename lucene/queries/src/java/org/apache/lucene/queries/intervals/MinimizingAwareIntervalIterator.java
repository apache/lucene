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

package org.apache.lucene.queries.intervals;

import java.io.IOException;

/**
 * Minimizing interval iterators often return from nextInterval when subIntervals moved from match
 * position. Interval match iterators cannot determine minimized interval position itself, but must
 * have a possibility to restore exact offsets inside the match.
 */
interface MinimizingAwareIntervalIterator {
  /**
   * Will be called by higher iterator when this iterator positioned at match position.
   *
   * <p>May be called several times during match minimization.
   */
  void matchFound() throws IOException;
}
