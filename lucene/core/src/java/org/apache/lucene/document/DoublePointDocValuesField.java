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

import java.util.Arrays;
import org.apache.lucene.util.NumericUtils;

/**
 * Takes an array of doubles and converts them to sortable longs, then stores as a {@link
 * BinaryDocValuesField}
 *
 * @lucene.experimental
 */
public class DoublePointDocValuesField extends BinaryDocValuesField {

  /**
   * Creates a new DoublePointFacetField, indexing the provided N-dimensional long point.
   *
   * @param name field name
   * @param point double[] value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public DoublePointDocValuesField(String name, double... point) {
    super(name, LongPoint.pack(convertToSortableLongPoint(point)));
  }

  private static long[] convertToSortableLongPoint(double[] point) {
    if (point == null || point.length == 0) {
      throw new IllegalArgumentException("Point value cannot be null or empty");
    }
    return Arrays.stream(point).mapToLong(NumericUtils::doubleToSortableLong).toArray();
  }
}
