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
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Writes vectors to an index. */
public abstract class KnnVectorsWriter implements Closeable {

  /** Sole constructor */
  protected KnnVectorsWriter() {}

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, KnnVectorsReader knnVectorsReader)
      throws IOException;

  /** Called once at the end before close */
  public abstract void finish() throws IOException;

  /**
   * Merges the segment vectors for all fields. This default implementation delegates to {@link
   * #writeField}, passing a {@link KnnVectorsReader} that combines the vector values and ignores
   * deleted documents.
   */
  public void merge(MergeState mergeState) throws IOException {
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

        writeField(
            fieldInfo,
            new KnnVectorsReader() {
              @Override
              public long ramBytesUsed() {
                return 0;
              }

              @Override
              public void close() {
                throw new UnsupportedOperationException();
              }

              @Override
              public void checkIntegrity() {
                throw new UnsupportedOperationException();
              }

              @Override
              public VectorValues getVectorValues(String field) throws IOException {
                return MergedVectorValues.mergeVectorValues(fieldInfo, mergeState);
              }

              @Override
              public TopDocs search(
                  String field, float[] target, int k, Bits acceptDocs, int visitedLimit) {
                throw new UnsupportedOperationException();
              }
            });

        if (mergeState.infoStream.isEnabled("VV")) {
          mergeState.infoStream.message("VV", "merge done " + mergeState.segmentInfo);
        }
      }
    }
    finish();
  }

  /** Tracks state of one sub-reader that we are merging */
  private static class VectorValuesSub extends DocIDMerger.Sub {

    final VectorValues values;

    VectorValuesSub(MergeState.DocMap docMap, VectorValues values) {
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
  private static class MergedVectorValues extends VectorValues {
    private final List<VectorValuesSub> subs;
    private final DocIDMerger<VectorValuesSub> docIdMerger;
    private final int cost;
    private final int size;

    private int docId;
    private VectorValuesSub current;

    /** Returns a merged view over all the segment's {@link VectorValues}. */
    static MergedVectorValues mergeVectorValues(FieldInfo fieldInfo, MergeState mergeState)
        throws IOException {
      assert fieldInfo != null && fieldInfo.hasVectorValues();

      List<VectorValuesSub> subs = new ArrayList<>();
      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        KnnVectorsReader knnVectorsReader = mergeState.knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          VectorValues values = knnVectorsReader.getVectorValues(fieldInfo.name);
          if (values != null) {
            subs.add(new VectorValuesSub(mergeState.docMaps[i], values));
          }
        }
      }
      return new MergedVectorValues(subs, mergeState);
    }

    private MergedVectorValues(List<VectorValuesSub> subs, MergeState mergeState)
        throws IOException {
      this.subs = subs;
      docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
      int totalCost = 0, totalSize = 0;
      for (VectorValuesSub sub : subs) {
        totalCost += sub.values.cost();
        totalSize += sub.values.size();
      }
      cost = totalCost;
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
    public long cost() {
      return cost;
    }

    @Override
    public int dimension() {
      return subs.get(0).values.dimension();
    }
  }
}
