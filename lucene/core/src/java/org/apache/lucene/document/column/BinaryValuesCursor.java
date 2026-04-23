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
package org.apache.lucene.document.column;

import org.apache.lucene.util.BytesRef;

/**
 * A values cursor over a dense {@link NumericBinaryColumn}. Each call to {@link #nextValues()}
 * returns the next chunk of packed bytes; the length of each returned {@link BytesRef} must be a
 * multiple of the column's {@link NumericBinaryColumn#fixedSize()}. Across all calls, exactly
 * {@code numDocs} values (chunks of length {@code fixedSize} each) must be produced.
 *
 * @lucene.experimental
 */
public abstract class BinaryValuesCursor {

  /** Sole constructor. */
  protected BinaryValuesCursor() {}

  /**
   * Returns the next chunk of packed bytes, or {@code null} when the cursor is exhausted. The
   * returned {@link BytesRef} is only valid until the next call to {@code nextBytes()}.
   */
  public abstract BytesRef nextValues();
}
