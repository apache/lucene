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
package org.apache.lucene.backward_codecs.lucene91;

/**
 * A helper class for an hnsw graph that serves as a comparator of the currently set bound value
 * with a new value.
 */
public abstract class Lucene91BoundsChecker {

  float bound;

  /** Default Constructor */
  public Lucene91BoundsChecker() {}

  /** Update the bound if sample is better */
  public abstract void update(float sample);

  /** Update the bound unconditionally */
  public void set(float sample) {
    bound = sample;
  }

  /**
   * Check the sample
   *
   * @param sample a score
   * @return whether the sample exceeds (is worse than) the bound
   */
  public abstract boolean check(float sample);

  /**
   * Create a min or max bound checker
   *
   * @param reversed true for the min and false for the max
   * @return the bound checker
   */
  public static Lucene91BoundsChecker create(boolean reversed) {
    if (reversed) {
      return new Min();
    } else {
      return new Max();
    }
  }

  /**
   * A helper class for an hnsw graph that serves as a comparator of the currently set maximum value
   * with a new value.
   */
  public static class Max extends Lucene91BoundsChecker {
    Max() {
      bound = Float.NEGATIVE_INFINITY;
    }

    @Override
    public void update(float sample) {
      if (sample > bound) {
        bound = sample;
      }
    }

    @Override
    public boolean check(float sample) {
      return sample < bound;
    }
  }

  /**
   * A helper class for an hnsw graph that serves as a comparator of the currently set minimum value
   * with a new value.
   */
  public static class Min extends Lucene91BoundsChecker {

    Min() {
      bound = Float.POSITIVE_INFINITY;
    }

    @Override
    public void update(float sample) {
      if (sample < bound) {
        bound = sample;
      }
    }

    @Override
    public boolean check(float sample) {
      return sample > bound;
    }
  }
}
