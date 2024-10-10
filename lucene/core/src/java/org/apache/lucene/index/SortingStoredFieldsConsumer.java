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
import java.util.Objects;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** A {@link StoredFieldsConsumer} that handles index sorts */
public abstract class SortingStoredFieldsConsumer extends StoredFieldsConsumer {
  private TrackingTmpOutputDirectoryWrapper tmpDirectory;
  private final StoredFieldsFormat tmpStoredFieldsFormat;

  /** Sole constructor */
  protected SortingStoredFieldsConsumer(
      Codec codec,
      StoredFieldsFormat tmpStoredFieldsFormat,
      Directory directory,
      SegmentInfo info) {
    super(codec, directory, info);
    this.tmpStoredFieldsFormat = tmpStoredFieldsFormat;
  }

  @Override
  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) {
      this.tmpDirectory = new TrackingTmpOutputDirectoryWrapper(directory);
      this.writer = tmpStoredFieldsFormat.fieldsWriter(tmpDirectory, info, IOContext.DEFAULT);
    }
  }

  @Override
  final void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    super.flush(state, sortMap);
    StoredFieldsReader reader =
        tmpStoredFieldsFormat.fieldsReader(
            tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
    // Don't pull a merge instance, since merge instances optimize for
    // sequential access while we consume stored fields in random order here.
    StoredFieldsWriter sortWriter =
        codec.storedFieldsFormat().fieldsWriter(state.directory, state.segmentInfo, state.context);
    try {
      reader.checkIntegrity();
      copyDocs(state, sortMap, sortWriter, reader);
      sortWriter.finish(state.segmentInfo.maxDoc());
    } finally {
      IOUtils.close(reader, sortWriter);
      IOUtils.deleteFiles(tmpDirectory, tmpDirectory.getTemporaryFiles().values());
    }
  }

  /** Copy documents from the temp stored fields reader to the final stored fields writer. */
  protected void copyDocs(
      SegmentWriteState state,
      Sorter.DocMap sortMap,
      StoredFieldsWriter sortWriter,
      StoredFieldsReader tempReader)
      throws IOException {
    CopyVisitor visitor = new CopyVisitor(sortWriter);
    for (int docID = 0; docID < state.segmentInfo.maxDoc(); docID++) {
      sortWriter.startDocument();
      tempReader.document(sortMap == null ? docID : sortMap.newToOld(docID), visitor);
      sortWriter.finishDocument();
    }
  }

  @Override
  void abort() {
    try {
      super.abort();
    } finally {
      if (tmpDirectory != null) {
        IOUtils.deleteFilesIgnoringExceptions(
            tmpDirectory, tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  /** A visitor that copies every field it sees in the provided {@link StoredFieldsWriter}. */
  private static class CopyVisitor extends StoredFieldVisitor {
    final StoredFieldsWriter writer;

    CopyVisitor(StoredFieldsWriter writer) {
      this.writer = writer;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, DataInput value, int length) throws IOException {
      writer.writeField(fieldInfo, value, length);
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      // TODO: can we avoid new BR here?
      writer.writeField(fieldInfo, new BytesRef(value));
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      writer.writeField(
          fieldInfo, Objects.requireNonNull(value, "String value should not be null"));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      writer.writeField(fieldInfo, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      writer.writeField(fieldInfo, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      writer.writeField(fieldInfo, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      writer.writeField(fieldInfo, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return Status.YES;
    }
  }
}
