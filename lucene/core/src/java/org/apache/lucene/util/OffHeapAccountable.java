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

import java.util.Collection;
import java.util.Collections;

/**
 * An object whose off-heap memory usage can be determined.
 *
 * <p>The value reported is not necessarily the actual RAM usage, but rather the size of off-heap
 * space required if it were to be fully loaded into memory.
 *
 * @lucene.experimental
 */
public interface OffHeapAccountable {

  /**
   * Returns the size of the off-heap memory usage in bytes.
   *
   * @return a non-negative value indicate the size of the off-heap memory usage.
   */
  long offHeapByteSize();

  /**
   * Returns nested resources of this class. The result should be a point-in-time snapshot (to avoid
   * race conditions).
   */
  default Collection<OffHeapAccountable> getChildOffHeapResources() {
    return Collections.emptyList();
  }

  static OffHeapAccountable named(String description, OffHeapAccountable in) {
    return named(
        description + " [" + in + "]", in.getChildOffHeapResources(), in.offHeapByteSize());
  }

  static OffHeapAccountable named(
      String description, Collection<OffHeapAccountable> children, long bytes) {
    return new OffHeapAccountable() {
      @Override
      public long offHeapByteSize() {
        return bytes;
      }

      @Override
      public Collection<OffHeapAccountable> getChildOffHeapResources() {
        return children;
      }

      @Override
      public String toString() {
        return description;
      }
    };
  }
}
