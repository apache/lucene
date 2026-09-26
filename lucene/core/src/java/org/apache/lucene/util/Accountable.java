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
 * An object whose JVM heap memory usage can be computed.
 *
 * <p>Implementations should only account for memory allocated on the Java heap (i.e. memory managed
 * by the JVM garbage collector). Off-heap resources such as memory-mapped files or native direct
 * buffers should not be included; those should be tracked separately if needed (see e.g. {@link
 * org.apache.lucene.codecs.KnnVectorsReader#getOffHeapByteSize}).
 *
 * @lucene.internal
 */
public interface Accountable {

  /**
   * Returns an estimate of the JVM heap memory used by this object in bytes. The method name uses
   * "ram" for historical reasons; only JVM heap memory should be reported. Off-heap resources such
   * as memory-mapped files or native direct buffers should not be included. Negative values are
   * illegal.
   *
   * <p>Implementations are encouraged to follow an ownership-oriented convention: report memory
   * allocated for this object's lifetime, or released by its {@code close()} method if applicable.
   * Inputs received via constructors or factories may be borrowed, wrapped, sliced, or copied;
   * implementations should report only the bytes they actually own, not the deep content of
   * referenced storage they do not own. The reference itself (the pointer slot for a borrowed
   * field) is part of this object's own layout and is naturally accounted via shallow size; what
   * should not be added is the deep content of the referenced storage. Reporting the deep content
   * of borrowed storage can lead to double counting when consumers sum across multiple Accountable
   * instances that share state.
   */
  long ramBytesUsed();

  /**
   * Returns nested resources of this class. The result should be a point-in-time snapshot (to avoid
   * race conditions).
   *
   * <p>This method is a diagnostic accessor intended for inspection and printing. The returned
   * collection is not a deduplicated ownership tree; it may include borrowed or shared references.
   * Summing {@link #ramBytesUsed()} across its elements is not generally sum-safe.
   *
   * @see Accountables
   */
  default Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /** An accountable that always returns 0 */
  Accountable NULL_ACCOUNTABLE = () -> 0;
}
