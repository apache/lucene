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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * This exception is thrown when Lucene is unable to read a SegmentInfo file _N.si (When it is
 * missing or corrupt)
 */
public class CorruptSegmentInfoException extends CorruptIndexException {

  // The segment name of the missing .si file is always stored for accurate error reporting
  String segmentName;

  /** Create exception with the segmentName and message only */
  public CorruptSegmentInfoException(String segmentName, String message, DataInput input) {
    super(message, input);
    this.segmentName = segmentName;
  }

  /** Create exception with the segmentName and message only */
  public CorruptSegmentInfoException(String segmentName, String message, DataOutput output) {
    super(message, output);
    this.segmentName = segmentName;
  }

  /** Create exception with the segmentName, message and root cause. */
  public CorruptSegmentInfoException(
      String segmentName, String message, DataInput input, Throwable cause) {
    super(message, input, cause);
    this.segmentName = segmentName;
  }

  /** Create exception with the segmentName, message and root cause. */
  public CorruptSegmentInfoException(
      String segmentName, String message, DataOutput output, Throwable cause) {
    super(message, output, cause);
    this.segmentName = segmentName;
  }

  /** Create exception with the segmentName and message only */
  public CorruptSegmentInfoException(
      String segmentName, String message, String resourceDescription) {
    super(message, resourceDescription);
    this.segmentName = segmentName;
  }

  /** Create exception with the segmentName, message and root cause. */
  public CorruptSegmentInfoException(
      String segmentName, String message, String resourceDescription, Throwable cause) {
    super(message, resourceDescription, cause);
    this.segmentName = segmentName;
  }
}
