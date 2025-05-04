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
package org.apache.lucene.store;

/** Advice regarding the read access pattern. */
public enum ReadAdvice {
  /**
   * Normal behavior. Data is expected to be read mostly sequentially. The system is expected to
   * cache the hottest pages.
   */
  NORMAL,
  /**
   * Data is expected to be read in a random-access fashion, either by {@link IndexInput#seek(long)
   * seeking} often and reading relatively short sequences of bytes at once, or by reading data
   * through the {@link RandomAccessInput} abstraction in random order.
   */
  RANDOM,
  /**
   * Data is expected to be read sequentially with very little seeking at most. The system may read
   * ahead aggressively and free pages soon after they are accessed.
   */
  SEQUENTIAL
}
