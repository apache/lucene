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

package org.apache.lucene.sandbox.codecs.segmentivf;

import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;

/** Builds formats with an exact cell count, for tests that need small, known cell counts. */
final class SegmentIVFTestUtil {
  private SegmentIVFTestUtil() {}

  static SegmentIVFVectorsFormat format(
      int nlist, int dim, int nprobe, int spillBits, FineTier fine, CoarseTier coarse) {
    // nlist is the square root of the maximum vector count, whatever the dimension.
    SegmentIVFVectorsFormat format =
        new SegmentIVFVectorsFormat(nlist * nlist, nprobe, spillBits, fine, coarse);
    assert format.nlist() == nlist;
    return format;
  }

  /** An S1, INT8 format with one spill cell. */
  static SegmentIVFVectorsFormat format(int nlist, int dim, int nprobe) {
    return format(nlist, dim, nprobe, 1, FineTier.INT8, CoarseTier.S1);
  }
}
