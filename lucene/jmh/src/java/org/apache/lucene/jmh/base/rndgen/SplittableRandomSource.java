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
package org.apache.lucene.jmh.base.rndgen;

import java.util.SplittableRandom;

/** The type Splittable random source. */
public class SplittableRandomSource implements RandomnessSource {

  /** The Random. */
  final SplittableRandom random;

  /**
   * Instantiates a new Splittable random source.
   *
   * @param random the random
   */
  public SplittableRandomSource(SplittableRandom random) {
    this.random = random;
  }

  @Override
  public long next(long min, long max) {
    return random.nextLong(min, max);
  }

  @Override
  public RandomnessSource withDistribution(Distribution distribution) {
    return this;
  }

  @Override
  public Distribution getDistribution() {
    return Distribution.UNIFORM;
  }
}
