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
package org.apache.lucene.util.search;

import java.io.IOException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.ArrayUtil;

/**
 * Utility methods for working with point-based queries in Lucene.
 *
 * <p>{@code PointQueryUtils} provides reusable static methods for validating point values and
 * determining the spatial relation between query ranges and indexed point value ranges. These
 * methods are used by point range queries and weights to ensure correctness and optimize query
 * execution.
 */
public final class PointQueryUtils {

  private PointQueryUtils() {}

  /**
   * Determines the spatial relation between a query range and a cell defined by min and max packed
   * values.
   *
   * @param minPackedValue minimum packed value of the cell
   * @param maxPackedValue maximum packed value of the cell
   * @param lowerPoint lower bound of the query range
   * @param upperPoint upper bound of the query range
   * @param numDims number of dimensions
   * @param bytesPerDim bytes per dimension
   * @param comparator comparator for byte arrays
   * @return the {@link Relation} between the cell and the query range
   */
  public static PointValues.Relation relate(
      byte[] minPackedValue,
      byte[] maxPackedValue,
      byte[] lowerPoint,
      byte[] upperPoint,
      int numDims,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    boolean crosses = false;
    int offset = 0;

    for (int dim = 0; dim < numDims; dim++, offset += bytesPerDim) {

      if (comparator.compare(minPackedValue, offset, upperPoint, offset) > 0
          || comparator.compare(maxPackedValue, offset, lowerPoint, offset) < 0) {
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }

      // Evaluate crosses only when false. Still need to iterate through
      // all the dimensions to ensure, none of them is completely outside
      if (!crosses) {
        crosses =
            comparator.compare(minPackedValue, offset, lowerPoint, offset) < 0
                || comparator.compare(maxPackedValue, offset, upperPoint, offset) > 0;
      }
    }

    if (crosses) {
      return PointValues.Relation.CELL_CROSSES_QUERY;
    } else {
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }
  }

  /**
   * Validates that the provided {@link PointValues} instance matches the expected field, number of
   * dimensions, and bytes per dimension.
   *
   * @param values the {@link PointValues} to validate
   * @param field the field name
   * @param numDims expected number of dimensions
   * @param bytesPerDim expected bytes per dimension
   * @return true if valid, false otherwise
   * @throws IllegalArgumentException if the dimensions or bytes per dimension do not match
   */
  public static boolean checkValidPointValues(
      PointValues values, String field, int numDims, int bytesPerDim) throws IOException {
    if (values == null) {
      // No docs in this segment/field indexed any points
      return false;
    }

    if (values.getNumIndexDimensions() != numDims) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" was indexed with numIndexDimensions="
              + values.getNumIndexDimensions()
              + " but this query has numDims="
              + numDims);
    }
    if (bytesPerDim != values.getBytesPerDimension()) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" was indexed with bytesPerDim="
              + values.getBytesPerDimension()
              + " but this query has bytesPerDim="
              + bytesPerDim);
    }
    return true;
  }
}
