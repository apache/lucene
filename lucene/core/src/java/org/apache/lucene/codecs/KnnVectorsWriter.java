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

package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.AbstractVectorValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;

/** Writes vectors to an index. */
public abstract class KnnVectorsWriter implements Accountable, Closeable {

  /** Sole constructor */
  protected KnnVectorsWriter() {}

  /** Add new field for indexing */
  public abstract KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException;

  /** Flush all buffered data on disk * */
  public abstract void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException;

  /** Write field for merging */
  @SuppressWarnings("unchecked")
  public <T> void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    KnnFieldVectorsWriter<T> writer = (KnnFieldVectorsWriter<T>) addField(fieldInfo);
    AbstractVectorValues<T> mergedValues =
        (AbstractVectorValues<T>) MergedVectorValues.mergeVectorValues(fieldInfo, mergeState);
    for (int doc = mergedValues.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = mergedValues.nextDoc()) {
      writer.addValue(doc, mergedValues.vectorValue());
    }
  }

  /** Called once at the end before close */
  public abstract void finish() throws IOException;

  /**
   * Merges the segment vectors for all fields. This default implementation delegates to {@link
   * #mergeOneField}, passing a {@link KnnVectorsReader} that combines the vector values and ignores
   * deleted documents.
   */
  public final void merge(MergeState mergeState) throws IOException {
    for (int i = 0; i < mergeState.fieldInfos.length; i++) {
      KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
      assert reader != null || mergeState.fieldInfos[i].hasVectorValues() == false;
      if (reader != null) {
        reader.checkIntegrity();
      }
    }

    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.hasVectorValues()) {
        if (mergeState.infoStream.isEnabled("VV")) {
          mergeState.infoStream.message("VV", "merging " + mergeState.segmentInfo);
        }

        mergeOneField(fieldInfo, mergeState);

        if (mergeState.infoStream.isEnabled("VV")) {
          mergeState.infoStream.message("VV", "merge done " + mergeState.segmentInfo);
        }
      }
    }
    finish();
  }

  /** Tracks state of one sub-reader that we are merging */
  private static class VectorValuesSub<T> extends DocIDMerger.Sub {

    final AbstractVectorValues<T> values;

    VectorValuesSub(MergeState.DocMap docMap, AbstractVectorValues<T> values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /** View over multiple VectorValues supporting iterator-style access via DocIdMerger. */
  protected static final class MergedVectorValues {
    private MergedVectorValues() {}

    static MergedFloat32VectorValues mergedFloat32VectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as FLOAT32");
      }
      List<VectorValuesSub<float[]>> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          AbstractVectorValues<float[]> values = knnVectorsReader.getVectorValues(fieldInfo.name);
          if (values != null) {
            subs.add(new VectorValuesSub<>(mergeState.docMaps[i], values));
          }
        }
      }
      return new MergedFloat32VectorValues(subs, mergeState);
    }

    private static MergedByteVectorValues mergedByteVectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      if (fieldInfo.getVectorEncoding() != VectorEncoding.BYTE) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as BYTE");
      }
      List<VectorValuesSub<BytesRef>> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          AbstractVectorValues<BytesRef> values =
              knnVectorsReader.getByteVectorValues(fieldInfo.name);
          if (values != null) {
            subs.add(new VectorValuesSub<>(mergeState.docMaps[i], values));
          }
        }
      }
      return new MergedByteVectorValues(subs, mergeState);
    }

    /** Returns a merged view over all the segment's {@link AbstractVectorValues}. */
    public static AbstractVectorValues<?> mergeVectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      return switch (fieldInfo.getVectorEncoding()) {
        case BYTE -> mergedByteVectorValues(fieldInfo, mergeState);
        case FLOAT32 -> mergedFloat32VectorValues(fieldInfo, mergeState);
      };
    }

    static class MergedFloat32VectorValues extends VectorValues {
      private final List<VectorValuesSub<float[]>> subs;
      private final DocIDMerger<VectorValuesSub<float[]>> docIdMerger;
      private final int size;

      private int docId;
      VectorValuesSub<float[]> current;

      private MergedFloat32VectorValues(List<VectorValuesSub<float[]>> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (VectorValuesSub<float[]> sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
        docId = -1;
      }

      @Override
      public int docID() {
        return docId;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIdMerger.next();
        if (current == null) {
          docId = NO_MORE_DOCS;
        } else {
          docId = current.mappedDocID;
        }
        return docId;
      }

      @Override
      public float[] vectorValue() throws IOException {
        return current.values.vectorValue();
      }

      @Override
      public BytesRef binaryValue() throws IOException {
        return current.values.binaryValue();
      }

      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return subs.get(0).values.dimension();
      }
    }

    static class MergedByteVectorValues extends ByteVectorValues {
      private final List<VectorValuesSub<BytesRef>> subs;
      private final DocIDMerger<VectorValuesSub<BytesRef>> docIdMerger;
      private final int size;

      private int docId;
      VectorValuesSub<BytesRef> current;

      private MergedByteVectorValues(List<VectorValuesSub<BytesRef>> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (VectorValuesSub<BytesRef> sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
        docId = -1;
      }

      @Override
      public BytesRef vectorValue() throws IOException {
        return current.values.vectorValue();
      }

      @Override
      public int docID() {
        return docId;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIdMerger.next();
        if (current == null) {
          docId = NO_MORE_DOCS;
        } else {
          docId = current.mappedDocID;
        }
        return docId;
      }

      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return subs.get(0).values.dimension();
      }
    }
  }
}
