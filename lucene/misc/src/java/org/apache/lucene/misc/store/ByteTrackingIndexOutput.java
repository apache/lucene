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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IndexOutput;

/** An {@link IndexOutput} that wraps another instance and tracks the number of bytes written */
public class ByteTrackingIndexOutput extends FilterIndexOutput {

  private final AtomicLong byteTracker;
  private boolean closed = false;

  protected ByteTrackingIndexOutput(IndexOutput out, AtomicLong byteTracker) {
    super(
        "Byte tracking wrapper for: " + out.getName(),
        "ByteTrackingIndexOutput{" + out.getName() + "}",
        out);
    this.byteTracker = byteTracker;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      out.close();
      return;
    }
    byteTracker.addAndGet(out.getFilePointer());
    closed = true;
    out.close();
  }
}
