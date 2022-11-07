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
package org.apache.lucene.document;

import org.apache.lucene.util.BytesRef;

/**
 * A subclass of DoubleRangeDocValuesField that only allows single dimension range representation.
 * Meant to be used with RangeOnRange faceting
 */
public class DoubleRangeDocValuesFacetField extends DoubleRangeDocValuesField {

  /**
   * Constructor for DoubleRangeDocValuesFacetField
   *
   * @param field the field name
   * @param min the min of the represented range
   * @param max the max of the represented range
   */
  public DoubleRangeDocValuesFacetField(String field, final double min, final double max) {
    super(field, new double[] {min}, new double[] {max});
  }

  /**
   * Sets the value of this field to a new value
   *
   * @param min the new min
   * @param max the new max
   */
  public void setDoubleRangeValue(double min, double max) {
    byte[] encodedValue = new byte[2 * Double.BYTES];
    DoubleRange.verifyAndEncode(new double[] {min}, new double[] {max}, encodedValue);
    setBytesValue(new BytesRef(encodedValue));
  }
}
