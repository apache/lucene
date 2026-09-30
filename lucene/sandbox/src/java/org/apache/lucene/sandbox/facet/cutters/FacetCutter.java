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
package org.apache.lucene.sandbox.facet.cutters;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;

/**
 * Creates {@link LeafFacetCutter} for each leaf.
 *
 * <p>TODO: do we need FacetCutterManager similar to CollectorManager, e.g. is createLeafCutter
 * always thread safe?
 *
 * @lucene.experimental
 */
public interface FacetCutter {

  /** Get cutter for the leaf. */
  LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException;

  /**
   * Returns true if this cutter records raw ordinals during collection that must be remapped to
   * final ordinals at reduce time via {@link #remapOrd}.
   *
   * <p>When false (the default), recorded ordinals are already final and the recorder skips the
   * remap step entirely.
   *
   * <p>Called once per {@link org.apache.lucene.sandbox.facet.recorders.FacetRecorder#reduce}
   * invocation.
   */
  default boolean needsRemapping() throws IOException {
    return false;
  }

  /**
   * Map a raw ordinal recorded during collection to zero or more final ordinals.
   *
   * <p>Only called when {@link #needsRemapping()} returns true. Called once per distinct recorded
   * ordinal inside {@link org.apache.lucene.sandbox.facet.recorders.FacetRecorder#reduce}.
   * Implementations must return a fresh or reset iterator on every invocation.
   *
   * @param mergedOrd raw ordinal as recorded by the leaf cutter
   * @return iterator over final ordinals; may be {@link OrdinalIterator#EMPTY}
   */
  default OrdinalIterator remapOrd(int mergedOrd) throws IOException {
    throw new UnsupportedOperationException(
        "remapOrd called on a cutter that does not override it; "
            + "needsRemapping() must return true only when remapOrd() is implemented");
  }
}
