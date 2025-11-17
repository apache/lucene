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

package org.apache.lucene.internal.hppc;

/**
 * Forked from HPPC, holding int index and int value.
 *
 * @lucene.internal
 */
public final class IntCursor {
  /**
   * The current value's index in the container this cursor belongs to. The meaning of this index is
   * defined by the container (usually it will be an index in the underlying storage buffer).
   */
  public int index;

  /** The current value. */
  public int value;

  @Override
  public String toString() {
    return "[cursor, index: " + index + ", value: " + value + "]";
  }
}
