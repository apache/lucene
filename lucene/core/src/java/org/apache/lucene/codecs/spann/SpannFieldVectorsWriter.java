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
package org.apache.lucene.codecs.spann;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/** Buffers vectors to a temporary file for SPANN clustered writing. */
public class SpannFieldVectorsWriter extends KnnFieldVectorsWriter<float[]> implements Closeable {
  private final FieldInfo fieldInfo;
  private final SegmentWriteState state;
  private final IndexOutput tempOut;
  private final String tempFileName;
  private int count = 0;
  private boolean finished = false;

  public SpannFieldVectorsWriter(FieldInfo fieldInfo, SegmentWriteState state) throws IOException {
    this.fieldInfo = fieldInfo;
    this.state = state;
    this.tempOut =
        state.directory.createTempOutput(
            state.segmentInfo.name + "_" + fieldInfo.name, "spann_buffer", state.context);
    this.tempFileName = tempOut.getName();
  }

  @Override
  public void addValue(int docID, float[] vectorValue) throws IOException {
    tempOut.writeInt(docID);
    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      for (float f : vectorValue) {
        tempOut.writeByte((byte) f);
      }
    } else {
      for (float f : vectorValue) {
        tempOut.writeInt(Float.floatToIntBits(f));
      }
    }
    count++;
  }

  @Override
  public float[] copyValue(float[] vectorValue) {
    return vectorValue.clone();
  }

  @Override
  public long ramBytesUsed() {
    return 0; // Off-heap
  }

  public void finish() throws IOException {
    if (!finished) {
      tempOut.close();
      finished = true;
    }
  }

  public IndexInput openInput() throws IOException {
    if (!finished) {
      finish();
    }
    return state.directory.openInput(tempFileName, state.context);
  }

  public int getCount() {
    return count;
  }

  public FieldInfo getFieldInfo() {
    return fieldInfo;
  }

  public String getTempFileName() {
    return tempFileName;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(tempOut);
    IOUtils.deleteFilesIgnoringExceptions(state.directory, tempFileName);
  }
}
