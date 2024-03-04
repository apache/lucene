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

/** Keeps highest results, first by largest int value, then tie-break by smallest ord. */
public class TopOrdAndIntQueue extends TopOrdAndNumberQueue {

  /** Sole constructor. */
  public TopOrdAndIntQueue(int topN) {
    super(topN);
  }

  /** Hols an int value and an ordinal. */
  public static class OrdAndInt extends OrdAndValue {
    int value;

    /** Default constructor. */
    public OrdAndInt() {}

    @Override
    public boolean lessThan(OrdAndValue other) {
      OrdAndInt otherOrdAndInt = (OrdAndInt) other;
      if (value < otherOrdAndInt.value) {
        return true;
      }
      if (value > otherOrdAndInt.value) {
        return false;
      }
      return ord > otherOrdAndInt.ord; // tie-break by smallest ord
    }

    @Override
    public Number getValue() {
      return value;
    }

    @Override
    public void setValue(Number value) {
      this.value = (int) value;
    }
  }

  @Override
  public OrdAndValue newOrdAndValue() {
    return new OrdAndInt();
  }

  @Override
  public Number zero() {
    return 0;
  }
}
