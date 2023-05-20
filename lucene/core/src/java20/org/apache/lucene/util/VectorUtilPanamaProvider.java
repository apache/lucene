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

/** A VectorUtil provider implementation that leverages the Panama Vector API. */
final class VectorUtilPanamaProvider implements VectorUtilProvider {

  static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

  VectorUtilPanamaProvider() {}

  @Override
  public float dotProduct(float[] a, float[] b) {
    int i = 0;
    float res = 0;
    // if the array size is large (2x platform vector size), its worth the overhead to vectorize
    // vector loop is unrolled a single time (2 accumulators in parallel)
    if (a.length >= 2 * SPECIES.length()) {
      FloatVector acc1 = FloatVector.zero(SPECIES);
      FloatVector acc2 = FloatVector.zero(SPECIES);
      int upperBound = SPECIES.loopBound(a.length - SPECIES.length());
      for (; i < upperBound; i += 2 * SPECIES.length()) {
        FloatVector va = FloatVector.fromArray(SPECIES, a, i);
        FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
        acc1 = acc1.add(va.mul(vb));
        FloatVector vc = FloatVector.fromArray(SPECIES, a, i + SPECIES.length());
        FloatVector vd = FloatVector.fromArray(SPECIES, b, i + SPECIES.length());
        acc2 = acc2.add(vc.mul(vd));
      }
      res += acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD);
    }
    for (; i < a.length; i++) {
      res += b[i] * a[i];
    }
    return res;
  }
}
