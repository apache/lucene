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
package org.apache.lucene.facet;

import org.apache.lucene.util.PriorityQueue;

/** Keeps highest results, first by largest value, then tie-break by smallest ord. */
public abstract class TopOrdAndNumberQueue extends PriorityQueue<TopOrdAndNumberQueue.OrdAndValue> {

  /** Holds a single entry. */
  public abstract static class OrdAndValue {

    /** Ordinal of the entry. */
    public int ord;

    /** Default constructor. */
    public OrdAndValue() {}

    /** Compares this object with the specified object for order. */
    public abstract boolean lessThan(OrdAndValue other);

    /** Get the value. */
    public abstract Number getValue();

    /** Set the value. */
    public abstract void setValue(Number value);
  }

  /** Sole constructor. */
  public TopOrdAndNumberQueue(int topN) {
    super(topN);
  }

  @Override
  public final boolean lessThan(
      TopOrdAndNumberQueue.OrdAndValue a, TopOrdAndNumberQueue.OrdAndValue b) {
    return a.lessThan(b);
  }

  /** Creates a new {@link OrdAndValue} of the appropriate type. */
  public abstract OrdAndValue newOrdAndValue();

  /** Create a zero value of the appropriate type. */
  public abstract Number zero();
}
