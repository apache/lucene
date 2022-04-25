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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/** <code>MaxFloatFunction</code> returns the max of its components. */
public class MaxFloatFunction extends MultiFloatFunction {
  public MaxFloatFunction(ValueSource[] sources) {
    super(sources);
  }

  @Override
  protected String name() {
    return "max";
  }

  @Override
  protected float func(int doc, FunctionValues[] valsArr) throws IOException {
    boolean noneFound = true;
    float val = Float.NEGATIVE_INFINITY;
    for (FunctionValues vals : valsArr) {
      final float v = vals.floatVal(doc);
      // If value is not found, floatVal returns 0.0f
      // It is expensive to check vals.exists upfront,
      // we only check when floatVal returns 0.0f to
      // determine if 0.0f is due to not found or is
      // the real value of the field in that doc.
      if (v != 0.0f || vals.exists(doc)) {
        noneFound = false;
        val = Math.max(v, val);
      }
    }
    if (noneFound) {
      return 0.0f;
    }
    return val;
  }

  /**
   * True if <em>any</em> of the specified <code>values</code> {@link FunctionValues#exists} for the
   * specified doc, else false.
   *
   * @see MultiFunction#anyExists
   */
  @Override
  protected boolean exists(int doc, FunctionValues[] valsArr) throws IOException {
    return MultiFunction.anyExists(doc, valsArr);
  }
}
