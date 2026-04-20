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
package org.apache.lucene.document;

import org.apache.lucene.util.LongsRef;

/**
 * A values cursor over a dense {@link LongColumn}. Each call to {@link #nextLongs()} returns the
 * next chunk of values for consecutive batch-local doc-ids starting at 0, until exhausted. Across
 * all calls, exactly {@code numDocs} values must be produced.
 *
 * @lucene.experimental
 */
public abstract class LongValuesCursor {

  /** Sole constructor. */
  protected LongValuesCursor() {}

  /**
   * Returns the next chunk of long values, or {@code null} when the cursor is exhausted. The
   * returned {@link LongsRef} is only valid until the next call to {@code nextLongs()}.
   */
  public abstract LongsRef nextLongs();
}
