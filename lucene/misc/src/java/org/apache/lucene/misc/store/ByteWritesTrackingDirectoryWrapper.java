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
  private final AtomicLong realTimeFlushedBytes = new AtomicLong();
  private final AtomicLong realTimeMergedBytes = new AtomicLong();

  public final boolean trackTempOutput;

  /**
   * Constructor defaults to not tracking temp outputs
   *
   * @param in input Directory
   */
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
    IndexOutput byteTrackingIndexOutput;
    if (ioContext.context.equals(IOContext.Context.FLUSH)) {
      byteTrackingIndexOutput =
          new ByteTrackingIndexOutput(output, flushedBytes, realTimeFlushedBytes);
    } else if (ioContext.context.equals(IOContext.Context.MERGE)) {
      byteTrackingIndexOutput =
          new ByteTrackingIndexOutput(output, mergedBytes, realTimeMergedBytes);
    } else {
      return output;
    }
    return byteTrackingIndexOutput;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext ioContext)
      throws IOException {
    IndexOutput output = in.createTempOutput(prefix, suffix, ioContext);
    if (trackTempOutput) {
      IndexOutput byteTrackingIndexOutput;
      if (ioContext.context.equals(IOContext.Context.FLUSH)) {
        byteTrackingIndexOutput =
            new ByteTrackingIndexOutput(output, flushedBytes, realTimeFlushedBytes);
      } else if (ioContext.context.equals(IOContext.Context.MERGE)) {
        byteTrackingIndexOutput =
            new ByteTrackingIndexOutput(output, mergedBytes, realTimeMergedBytes);
      } else {
        return output;
      }
      return byteTrackingIndexOutput;
    }
    return output;
  }

  public double getApproximateWriteAmplificationFactor() {
    double flushedBytes = (double) this.flushedBytes.get();
    if (flushedBytes == 0.0) {
      return 1.0;
    }
    double mergedBytes = (double) this.mergedBytes.get();
    return (flushedBytes + mergedBytes) / flushedBytes;
  }

  /** Gets a more up-to-date but less accurate write amplification factor */
  public double getRealTimeApproximateWriteAmplificationFactor() {
    double flushedBytes = (double) this.realTimeFlushedBytes.get();
    if (flushedBytes == 0.0) {
      return 1.0;
    }
    double mergedBytes = (double) this.realTimeMergedBytes.get();
    return (flushedBytes + mergedBytes) / flushedBytes;
  }

  public long getFlushedBytes() {
    return flushedBytes.get();
  }

  public long getMergedBytes() {
    return mergedBytes.get();
  }
}
