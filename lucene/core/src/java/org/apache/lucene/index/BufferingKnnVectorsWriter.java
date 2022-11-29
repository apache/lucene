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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Buffers up pending vector value(s) per doc, then flushes when segment flushes. Used for {@code
 * SimpleTextKnnVectorsWriter} and for vectors writers before v 9.3 .
 *
 * @lucene.experimental
 */
public abstract class BufferingKnnVectorsWriter extends KnnVectorsWriter {
  private final List<FieldWriter> fields = new ArrayList<>();

  /** Sole constructor */
  protected BufferingKnnVectorsWriter() {}

  @Override
  public KnnFieldVectorsWriter<float[]> addField(FieldInfo fieldInfo) throws IOException {
    FieldWriter newField = new FieldWriter(fieldInfo);
    fields.add(newField);
    return newField;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter fieldData : fields) {
      fieldData.finaliseOrdsStructures();
      KnnVectorsReader knnVectorsReader =
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
              VectorValues vectorValues =
                  new BufferedVectorValues(
                      fieldData.docsWithField,
                      fieldData.vectors,
                      fieldData.fieldInfo.getVectorDimension(),
                          fieldData.finalOrds,
                          fieldData.finalOrdCounts);
              return sortMap != null
                  ? new VectorValues.SortingVectorValues(vectorValues, sortMap)
                  : vectorValues;
            }

            @Override
            public TopDocs search(
                String field, float[] target, int k, Bits acceptDocs, int visitedLimit, HnswGraphSearcher.Multivalued strategy) {
              throw new UnsupportedOperationException();
            }
          };

      writeField(fieldData.fieldInfo, knnVectorsReader, maxDoc);
    }
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (FieldWriter field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    KnnVectorsReader knnVectorsReader =
        new KnnVectorsReader() {

          @Override
          public long ramBytesUsed() {
            return 0;
          }

          @Override
          public void close() {}

          @Override
          public TopDocs search(
              String field, float[] target, int k, Bits acceptDocs, int visitedLimit,  HnswGraphSearcher.Multivalued strategy) {
            throw new UnsupportedOperationException();
          }

          @Override
          public VectorValues getVectorValues(String field) throws IOException {
            return MergedVectorValues.mergeVectorValues(fieldInfo, mergeState);
          }

          @Override
          public void checkIntegrity() {}
        };
    writeField(fieldInfo, knnVectorsReader, mergeState.segmentInfo.maxDoc());
  }

  /** Write the provided field */
  protected abstract void writeField(
      FieldInfo fieldInfo, KnnVectorsReader knnVectorsReader, int maxDoc) throws IOException;

  private static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
    private final PackedLongValues.Builder allVectorIds; // stream of all vectorIDs
    private PackedLongValues.Builder perDocumentVectors; // vectorIDs per doc
    private int currentDoc = -1;
    private int currentVectorId = 0;
    private int[] currentValues = new int[8];
    private int currentUpto;
    private int maxCount;

    private int[] finalSortedValues;
    
    private final FieldInfo fieldInfo;
    private final int dim;
    private final DocsWithFieldSet docsWithField;
    private final List<float[]> vectors;
    private PackedLongValues finalOrds;
    private PackedLongValues finalOrdCounts;

    private int lastDocID = -1;

    public FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      this.allVectorIds = PackedLongValues.packedBuilder(PackedInts.COMPACT);
      this.dim = fieldInfo.getVectorDimension();
      this.docsWithField = new DocsWithFieldSet();
      vectors = new ArrayList<>();
    }

    @Override
    public void addValue(int docID, Object value) {
      assert docID > lastDocID;
      float[] vectorValue =
          switch (fieldInfo.getVectorEncoding()) {
            case FLOAT32 -> (float[]) value;
            case BYTE -> bytesToFloats((BytesRef) value);
          };
      docsWithField.add(docID);
      if (docID != currentDoc) {
        finishCurrentDoc();
        currentDoc = docID;
      }
      increaseCounters();
      vectors.add(copyValue(vectorValue));
      lastDocID = docID;
    }

    // finalize currentDoc: this deduplicates the current term ids
    private void finishCurrentDoc() {
      if (currentDoc == -1) {
        return;
      }
      Arrays.sort(currentValues, 0, currentUpto);
      int currentDocumentVectors = 0;
      for (int i = 0; i < currentUpto; i++) {
        int vectorId = currentValues[i];
        allVectorIds.add(vectorId);
      }
      currentDocumentVectors = currentUpto;
      if (perDocumentVectors != null) {
        perDocumentVectors.add(currentDocumentVectors);
      } else if (currentDocumentVectors != 1) {
        perDocumentVectors = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);//??
        for (int i = 0; i < docsWithField.cardinality(); ++i) {
          perDocumentVectors.add(1);
        }
        perDocumentVectors.add(currentDocumentVectors);
      }
      maxCount = Math.max(maxCount, currentDocumentVectors);
      currentUpto = 0;
      docsWithField.add(currentDoc);
    }

    private void increaseCounters() {
      int vectorId = currentVectorId ++;

      if (currentUpto == currentValues.length) {
        currentValues = ArrayUtil.grow(currentValues, currentValues.length + 1);
      }

      currentValues[currentUpto] = vectorId;
      currentUpto++;
    }

    private float[] bytesToFloats(BytesRef b) {
      // This is used only by SimpleTextKnnVectorsWriter
      float[] floats = new float[dim];
      for (int i = 0; i < dim; i++) {
        floats[i] = b.bytes[i + b.offset];
      }
      return floats;
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      return ArrayUtil.copyOfSubArray(vectorValue, 0, dim);
    }

    @Override
    public long ramBytesUsed() {
      if (vectors.size() == 0) return 0;
      return allVectorIds.ramBytesUsed()
              + (perDocumentVectors == null ? 0 : perDocumentVectors.ramBytesUsed())
              + docsWithField.ramBytesUsed()
              + RamUsageEstimator.sizeOf(currentValues)
          + vectors.size()
              * (long)
                  (RamUsageEstimator.NUM_BYTES_OBJECT_REF
                      + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
    }

    public void finaliseOrdsStructures() {
      if (finalOrds == null) {
        assert finalOrdCounts == null;
        finishCurrentDoc();
        finalOrds = allVectorIds.build();
        finalOrdCounts = perDocumentVectors == null ? null : perDocumentVectors.build();
      } 
    }
  }

  private static class BufferedVectorValues extends VectorValues
      implements RandomAccessVectorValues {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final int dimension;

    final ByteBuffer buffer;
    final BytesRef binaryValue;
    final ByteBuffer raBuffer;
    final BytesRef raBinaryValue;

    final PackedLongValues ords;
    final PackedLongValues ordCounts;
    final PackedLongValues.Iterator vectorIdsIter;
    final PackedLongValues.Iterator vectorIdsPerDocumentCountIterator;
    DocIdSetIterator docsWithFieldIter;
    final int[] currentDocVectorsId;
    
    private int vectorsPerDocumentCount;
    private int visitedVectorIdPerCurrentDoc;

    BufferedVectorValues(DocsWithFieldSet docsWithField, List<float[]> vectors, int dimension, PackedLongValues ords, PackedLongValues ordCounts) {
      this.ords = ords;
      this.ordCounts = ordCounts;
      this.docsWithField = docsWithField;
      
      this.currentDocVectorsId = new int[this.maxVectorsPerDocument(docsWithField.getValuesPerDocument())];//should be the max recorded per document
      this.vectors = vectors;
      this.dimension = dimension;
      
      buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      binaryValue = new BytesRef(buffer.array());
      raBuffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      raBinaryValue = new BytesRef(raBuffer.array());
      
      this.vectorIdsIter = ords.iterator();
      this.vectorIdsPerDocumentCountIterator = ordCounts.iterator();
      docsWithFieldIter = docsWithField.iterator();
    }

    private int maxVectorsPerDocument(int[] valuesPerDocument) {
      int max = valuesPerDocument[0];
      for (int i = 1; i < valuesPerDocument.length; i++) {
        max = Math.max(max, valuesPerDocument[i]);
      }
      return max;
    }

    @Override
    public RandomAccessVectorValues copy() {
      return new BufferedVectorValues(docsWithField, vectors, dimension, ords, ordCounts);
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public BytesRef binaryValue() {
      buffer.asFloatBuffer().put(vectorValue());
      return binaryValue;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) {
      raBuffer.asFloatBuffer().put(vectors.get(targetOrd));
      return raBinaryValue;
    }

    @Override
    public float[] vectorValue() {
     return vectors.get(currentDocVectorsId[visitedVectorIdPerCurrentDoc-1]);
    }

    @Override
    public float[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public int docID() {
      return docsWithFieldIter.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithFieldIter.nextDoc();
      if (docID != NO_MORE_DOCS) {
        vectorsPerDocumentCount = (int) vectorIdsPerDocumentCountIterator.next();
        assert vectorsPerDocumentCount > 0;
        for (int i = 0; i < vectorsPerDocumentCount; i++) {
          currentDocVectorsId[i] = Math.toIntExact(vectorIdsIter.next());
        }
        visitedVectorIdPerCurrentDoc = 0;
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }
  }
}
