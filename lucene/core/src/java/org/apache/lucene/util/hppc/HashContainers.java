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

package org.apache.lucene.util.hppc;

import java.util.concurrent.atomic.AtomicInteger;

/** Constants for primitive maps. */
public class HashContainers {

  public static final int DEFAULT_EXPECTED_ELEMENTS = 4;

  public static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /** Minimal sane load factor (99 empty slots per 100). */
  public static final float MIN_LOAD_FACTOR = 1 / 100.0f;

  /** Maximum sane load factor (1 empty slot per 100). */
  public static final float MAX_LOAD_FACTOR = 99 / 100.0f;

  /** Minimum hash buffer size. */
  public static final int MIN_HASH_ARRAY_LENGTH = 4;

  /**
   * Maximum array size for hash containers (power-of-two and still allocable in Java, not a
   * negative int).
   */
  public static final int MAX_HASH_ARRAY_LENGTH = 0x80000000 >>> 1;

  static final AtomicInteger ITERATION_SEED = new AtomicInteger();
}
