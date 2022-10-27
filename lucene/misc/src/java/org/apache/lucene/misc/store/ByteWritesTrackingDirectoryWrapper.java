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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/** {@link FilterDirectory} that tracks write amplification factor */
public final class ByteWritesTrackingDirectoryWrapper extends FilterDirectory {

  private final AtomicLong flushedBytes = new AtomicLong();
  private final AtomicLong mergedBytes = new AtomicLong();
  public final boolean trackTempOutput;

  public ByteWritesTrackingDirectoryWrapper(Directory in) {
    this(in, false);
  }
  /**
   * Constructor with option to track tempOutput
   *
   * @param in input Directory
   * @param trackTempOutput if true, will also track temporary outputs created by this directory
   */
  public ByteWritesTrackingDirectoryWrapper(Directory in, boolean trackTempOutput) {
    super(in);
    this.trackTempOutput = trackTempOutput;
  }

  @Override
  public IndexOutput createOutput(String name, IOContext ioContext) throws IOException {
    IndexOutput output = in.createOutput(name, ioContext);
    return createByteTrackingOutput(output, ioContext.context);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext)
      throws IOException {
    IndexOutput output = in.createTempOutput(prefix, suffix, ioContext);
    return trackTempOutput ? createByteTrackingOutput(output, ioContext.context) : output;
  }

  private IndexOutput createByteTrackingOutput(IndexOutput output, IOContext.Context context) {
    switch (context) {
      case FLUSH:
        return new ByteTrackingIndexOutput(output, flushedBytes);
      case MERGE:
        return new ByteTrackingIndexOutput(output, mergedBytes);
      case DEFAULT:
      case READ:
      default:
        return output;
    }
  }

  public long getFlushedBytes() {
    return flushedBytes.get();
  }

  public long getMergedBytes() {
    return mergedBytes.get();
  }
}
