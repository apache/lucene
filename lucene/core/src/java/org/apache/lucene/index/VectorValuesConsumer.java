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

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/**
 * Streams vector values for indexing to the given codec's vectors writer. The codec's vectors
 * writer is responsible for buffering and processing vectors.
 */
class VectorValuesConsumer {
  private final Codec codec;
  private final Directory directory;
  private final SegmentInfo segmentInfo;
  private final InfoStream infoStream;

  private Accountable accountable = Accountable.NULL_ACCOUNTABLE;
  private KnnVectorsWriter writer;

  VectorValuesConsumer(
      Codec codec, Directory directory, SegmentInfo segmentInfo, InfoStream infoStream) {
    this.codec = codec;
    this.directory = directory;
    this.segmentInfo = segmentInfo;
    this.infoStream = infoStream;
  }

  private void initKnnVectorsWriter(String fieldName) throws IOException {
    if (writer == null) {
      KnnVectorsFormat fmt = codec.knnVectorsFormat();
      if (fmt == null) {
        throw new IllegalStateException(
            "field=\""
                + fieldName
                + "\" was indexed as vectors but codec does not support vectors");
      }
      SegmentWriteState initialWriteState =
          new SegmentWriteState(infoStream, directory, segmentInfo, null, null, IOContext.DEFAULT);
      writer = fmt.fieldsWriter(initialWriteState);
      accountable = writer;
    }
  }

  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    initKnnVectorsWriter(fieldInfo.name);
    return writer.addField(fieldInfo);
  }

  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    if (writer == null) return;
    try {
      writer.flush(state.segmentInfo.maxDoc(), sortMap);
      writer.finish();
    } finally {
      IOUtils.close(writer);
    }
  }

  void abort() {
    IOUtils.closeWhileHandlingException(writer);
  }

  public Accountable getAccountable() {
    return accountable;
  }
}
