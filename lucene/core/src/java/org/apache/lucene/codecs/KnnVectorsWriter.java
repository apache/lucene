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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;

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
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE:
        KnnFieldVectorsWriter<byte[]> byteWriter =
            (KnnFieldVectorsWriter<byte[]>) addField(fieldInfo);
        ByteVectorValues mergedBytes =
            MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        for (int doc = mergedBytes.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = mergedBytes.nextDoc()) {
          byteWriter.addValue(doc, mergedBytes.vectorValue());
        }
        break;
      case FLOAT32:
        KnnFieldVectorsWriter<float[]> floatWriter =
            (KnnFieldVectorsWriter<float[]>) addField(fieldInfo);
        FloatVectorValues mergedFloats =
            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        for (int doc = mergedFloats.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = mergedFloats.nextDoc()) {
          floatWriter.addValue(doc, mergedFloats.vectorValue());
        }
        break;
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
  private static class VectorValuesSub extends DocIDMerger.Sub {

    final FloatVectorValues values;

    VectorValuesSub(MergeState.DocMap docMap, FloatVectorValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  private static class ByteVectorValuesSub extends DocIDMerger.Sub {

    final ByteVectorValues values;

    ByteVectorValuesSub(MergeState.DocMap docMap, ByteVectorValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /** View over multiple vector values supporting iterator-style access via DocIdMerger. */
  protected static final class MergedVectorValues {
    private MergedVectorValues() {}

    /** Returns a merged view over all the segment's {@link FloatVectorValues}. */
    public static FloatVectorValues mergeFloatVectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      if (fieldInfo.getVectorEncoding() != VectorEncoding.FLOAT32) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as FLOAT32");
      }
      List<VectorValuesSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          FloatVectorValues values = knnVectorsReader.getFloatVectorValues(fieldInfo.name);
          if (values != null) {
            subs.add(new VectorValuesSub(mergeState.docMaps[i], values));
          }
        }
      }
      return new MergedFloat32VectorValues(subs, mergeState);
    }

    /** Returns a merged view over all the segment's {@link ByteVectorValues}. */
    public static ByteVectorValues mergeByteVectorValues(FieldInfo fieldInfo, MergeState mergeState)
        throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      if (fieldInfo.getVectorEncoding() != VectorEncoding.BYTE) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as [" + fieldInfo.getVectorEncoding() + "] as BYTE");
      }
      List<ByteVectorValuesSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          ByteVectorValues values = knnVectorsReader.getByteVectorValues(fieldInfo.name);
          if (values != null) {
            subs.add(new ByteVectorValuesSub(mergeState.docMaps[i], values));
          }
        }
      }
      return new MergedByteVectorValues(subs, mergeState);
    }

    static class MergedFloat32VectorValues extends FloatVectorValues {
      private final List<VectorValuesSub> subs;
      private final DocIDMerger<VectorValuesSub> docIdMerger;
      private final int size;

      private int docId;
      VectorValuesSub current;

      private MergedFloat32VectorValues(List<VectorValuesSub> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (VectorValuesSub sub : subs) {
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
      private final List<ByteVectorValuesSub> subs;
      private final DocIDMerger<ByteVectorValuesSub> docIdMerger;
      private final int size;

      private int docId;
      ByteVectorValuesSub current;

      private MergedByteVectorValues(List<ByteVectorValuesSub> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (ByteVectorValuesSub sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
        docId = -1;
      }

      @Override
      public byte[] vectorValue() throws IOException {
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
