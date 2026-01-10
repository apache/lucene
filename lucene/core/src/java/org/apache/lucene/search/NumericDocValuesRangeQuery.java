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

package org.apache.lucene.search;

import java.util.Objects;

/** A Query over a range of long values */
public abstract class NumericDocValuesRangeQuery extends Query {

  protected final String field;
  protected final long lowerValue;
  protected final long upperValue;

  protected NumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
  }

  /**
   * @return the field this Query applies to
   */
  public final String getField() {
    return field;
  }

  /**
   * @return the lower bound value
   */
  public final long lowerValue() {
    return lowerValue;
  }

  /**
   * @return the upper bound value
   */
  public final long upperValue() {
    return upperValue;
  }
}
