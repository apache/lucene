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
package org.apache.lucene.util;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class JDKVectorUtilProvider implements VectorUtil.VectorUtilProvider {

  static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  public JDKVectorUtilProvider() {}

  @Override
  public float dotProduct(float[] a, float[] b) {
    var upperBound = SPECIES.loopBound(a.length);
    var sum = FloatVector.zero(SPECIES);
    var i = 0;
    for (; i < upperBound; i += SPECIES.length()) {
      var va = FloatVector.fromArray(SPECIES, a, i);
      var vb = FloatVector.fromArray(SPECIES, b, i);
      sum = va.fma(vb, sum);
    }
    var c = sum.reduceLanes(VectorOperators.ADD);
    for (; i < a.length; i++) { // tail loop
      c += a[i] * b[i];
    }
    return c;
  }
}
