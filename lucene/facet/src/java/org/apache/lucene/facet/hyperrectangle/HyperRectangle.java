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
package org.apache.lucene.facet.hyperrectangle;

/** Holds the name and the number of dims for a HyperRectangle */
public abstract class HyperRectangle {
  /** Label that identifies this range. */
  public final String label;

  /** How many dimensions this hyper rectangle has (IE: a regular rectangle would have dims=2) */
  public final int dims;

  /** Sole constructor. */
  protected HyperRectangle(String label, int dims) {
    if (label == null) {
      throw new NullPointerException("label must not be null");
    }
    this.label = label;
    this.dims = dims;
  }

  /**
   * Converts hyper rectangles ranges into a comparable long from whatever type it is in
   *
   * @param dim dimension of the request range
   * @return The comparable long version of the requested range
   */
  public abstract LongHyperRectangle.LongRangePair getComparableDimRange(int dim);
}
