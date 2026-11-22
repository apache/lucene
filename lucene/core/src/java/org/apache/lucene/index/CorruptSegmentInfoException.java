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
package org.apache.lucene.index;

import java.util.Objects;
import org.apache.lucene.store.DataInput;

/**
 * Thrown when a single segment's {@code _N.si} file cannot be read, because it is missing or
 * corrupt, while the commit point that references it parsed far enough to name the segment.
 *
 * <p>Unlike a plain {@link CorruptIndexException}, this carries the {@link #getSegmentName() name
 * of the affected segment}, so a caller such as {@link CheckIndex} can report which segment is
 * broken rather than only that something was. The root cause is always attached, since it is what
 * names the file on disk.
 *
 * @lucene.internal
 */
public class CorruptSegmentInfoException extends CorruptIndexException {

  /** Name of the segment whose {@code .si} file could not be read. */
  private final String segmentName;

  /**
   * Create an exception naming the segment whose {@code .si} could not be read.
   *
   * @param segmentName name of the affected segment, must not be null
   * @param message description of what went wrong
   * @param input the input being read when the failure was detected
   * @param cause the underlying failure, must not be null
   */
  public CorruptSegmentInfoException(
      String segmentName, String message, DataInput input, Throwable cause) {
    super(message, input, Objects.requireNonNull(cause));
    this.segmentName = Objects.requireNonNull(segmentName);
  }

  /** Returns the name of the segment whose {@code .si} file could not be read. */
  public String getSegmentName() {
    return segmentName;
  }
}
