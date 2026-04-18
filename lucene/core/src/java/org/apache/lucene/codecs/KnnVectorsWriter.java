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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOFunction;

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
      case BYTE -> {
        KnnFieldVectorsWriter<byte[]> byteWriter =
            (KnnFieldVectorsWriter<byte[]>) addField(fieldInfo);
        ByteVectorValues mergedBytes =
            MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
        KnnVectorValues.DocIndexIterator iter = mergedBytes.iterator();
        for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
          byteWriter.addValue(doc, mergedBytes.vectorValue(iter.index()));
        }
      }
      case FLOAT32 -> {
        KnnFieldVectorsWriter<float[]> floatWriter =
            (KnnFieldVectorsWriter<float[]>) addField(fieldInfo);
        FloatVectorValues mergedFloats =
            MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        KnnVectorValues.DocIndexIterator iter = mergedFloats.iterator();
        for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
          floatWriter.addValue(doc, mergedFloats.vectorValue(iter.index()));
        }
      }
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

  /**
   * Tracks state of one sub-reader of float vectors that we are merging.
   *
   * @lucene.internal
   */
  static class FloatVectorValuesSub extends DocIDMerger.Sub {

    final FloatVectorValues values;
    final KnnVectorValues.DocIndexIterator iterator;

    FloatVectorValuesSub(MergeState.DocMap docMap, FloatVectorValues values) {
      super(docMap);
      this.values = values;
      this.iterator = values.iterator();
      assert iterator.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }

    public int index() {
      return iterator.index();
    }
  }

  /**
   * Tracks state of one sub-reader of byte vectors that we are merging.
   *
   * @lucene.internal
   */
  static class ByteVectorValuesSub extends DocIDMerger.Sub {

    final ByteVectorValues values;
    final KnnVectorValues.DocIndexIterator iterator;

    ByteVectorValuesSub(MergeState.DocMap docMap, ByteVectorValues values) {
      super(docMap);
      this.values = values;
      iterator = values.iterator();
      assert iterator.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }

    int index() {
      return iterator.index();
    }
  }

  /**
   * Given old doc ids and an id mapping, maps old ordinal to new ordinal. Note: this method return
   * nothing and output are written to parameters
   *
   * @param oldDocIds the old or current document ordinals. Must not be null.
   * @param sortMap the document sorting map for how to make the new ordinals. Must not be null.
   * @param old2NewOrd int[] maps from old ord to new ord
   * @param new2OldOrd int[] maps from new ord to old ord
   * @param newDocsWithField set of new doc ids which has the value
   */
  public static void mapOldOrdToNewOrd(
      DocsWithFieldSet oldDocIds,
      Sorter.DocMap sortMap,
      int[] old2NewOrd,
      int[] new2OldOrd,
      DocsWithFieldSet newDocsWithField)
      throws IOException {
    // TODO: a similar function exists in IncrementalHnswGraphMerger#getNewOrdMapping
    //       maybe we can do a further refactoring
    Objects.requireNonNull(oldDocIds);
    Objects.requireNonNull(sortMap);
    assert (old2NewOrd != null || new2OldOrd != null || newDocsWithField != null);
    assert (old2NewOrd == null || old2NewOrd.length == oldDocIds.cardinality());
    assert (new2OldOrd == null || new2OldOrd.length == oldDocIds.cardinality());
    IntIntHashMap newIdToOldOrd = new IntIntHashMap();
    DocIdSetIterator iterator = oldDocIds.iterator();
    int[] newDocIds = new int[oldDocIds.cardinality()];
    int oldOrd = 0;
    for (int oldDocId = iterator.nextDoc();
        oldDocId != DocIdSetIterator.NO_MORE_DOCS;
        oldDocId = iterator.nextDoc()) {
      int newId = sortMap.oldToNew(oldDocId);
      newIdToOldOrd.put(newId, oldOrd);
      newDocIds[oldOrd] = newId;
      oldOrd++;
    }

    Arrays.sort(newDocIds);
    int newOrd = 0;
    for (int newDocId : newDocIds) {
      int currOldOrd = newIdToOldOrd.get(newDocId);
      if (old2NewOrd != null) {
        old2NewOrd[currOldOrd] = newOrd;
      }
      if (new2OldOrd != null) {
        new2OldOrd[newOrd] = currOldOrd;
      }
      if (newDocsWithField != null) {
        newDocsWithField.add(newDocId);
      }
      newOrd++;
    }
  }

  /** View over multiple vector values supporting iterator-style access via DocIdMerger. */
  public static final class MergedVectorValues {
    private MergedVectorValues() {}

    private static void validateFieldEncoding(FieldInfo fieldInfo, VectorEncoding expected) {
      assert fieldInfo != null && fieldInfo.hasVectorValues();
      VectorEncoding fieldEncoding = fieldInfo.getVectorEncoding();
      if (fieldEncoding != expected) {
        throw new UnsupportedOperationException(
            "Cannot merge vectors encoded as [" + fieldEncoding + "] as " + expected);
      }
    }

    /**
     * Returns true if the fieldInfos has vector values for the field.
     *
     * @param fieldInfos fieldInfos for the segment
     * @param fieldName field name
     * @return true if the fieldInfos has vector values for the field.
     */
    public static boolean hasVectorValues(FieldInfos fieldInfos, String fieldName) {
      if (fieldInfos.hasVectorValues() == false) {
        return false;
      }
      FieldInfo info = fieldInfos.fieldInfo(fieldName);
      return info != null && info.hasVectorValues();
    }

    private static <V, S> List<S> mergeVectorValues(
        KnnVectorsReader[] knnVectorsReaders,
        MergeState.DocMap[] docMaps,
        FieldInfo mergingField,
        FieldInfos[] sourceFieldInfos,
        IOFunction<KnnVectorsReader, V> valuesSupplier,
        BiFunction<MergeState.DocMap, V, S> newSub)
        throws IOException {
      List<S> subs = new ArrayList<>();
      for (int i = 0; i < knnVectorsReaders.length; i++) {
        FieldInfos sourceFieldInfo = sourceFieldInfos[i];
        if (hasVectorValues(sourceFieldInfo, mergingField.name) == false) {
          continue;
        }
        KnnVectorsReader knnVectorsReader = knnVectorsReaders[i];
        if (knnVectorsReader != null) {
          V values = valuesSupplier.apply(knnVectorsReader);
          if (values != null) {
            subs.add(newSub.apply(docMaps[i], values));
          }
        }
      }
      return subs;
    }

    /** Returns a merged view over all the segment's {@link FloatVectorValues}. */
    public static FloatVectorValues mergeFloatVectorValues(
        FieldInfo fieldInfo, MergeState mergeState) throws IOException {
      validateFieldEncoding(fieldInfo, VectorEncoding.FLOAT32);
      return new MergedFloat32VectorValues(
          mergeVectorValues(
              mergeState.knnVectorsReaders,
              mergeState.docMaps,
              fieldInfo,
              mergeState.fieldInfos,
              knnVectorsReader -> knnVectorsReader.getFloatVectorValues(fieldInfo.name),
              FloatVectorValuesSub::new),
          mergeState);
    }

    /** Returns a merged view over all the segment's {@link ByteVectorValues}. */
    public static ByteVectorValues mergeByteVectorValues(FieldInfo fieldInfo, MergeState mergeState)
        throws IOException {
      validateFieldEncoding(fieldInfo, VectorEncoding.BYTE);
      return new MergedByteVectorValues(
          mergeVectorValues(
              mergeState.knnVectorsReaders,
              mergeState.docMaps,
              fieldInfo,
              mergeState.fieldInfos,
              knnVectorsReader -> knnVectorsReader.getByteVectorValues(fieldInfo.name),
              ByteVectorValuesSub::new),
          mergeState);
    }

    /**
     * Unified view over several segments containing float vector values.
     *
     * @lucene.internal
     */
    static class MergedFloat32VectorValues extends FloatVectorValues {
      private final List<FloatVectorValuesSub> subs;
      private final DocIDMerger<FloatVectorValuesSub> docIdMerger;
      private final int size;
      private int docId = -1;
      private int lastOrd = -1;
      FloatVectorValuesSub current;

      // package-private for testing
      MergedFloat32VectorValues(List<FloatVectorValuesSub> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (FloatVectorValuesSub sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
      }

      @Override
      public DocIndexIterator iterator() {
        return new DocIndexIterator() {
          private int index = -1;

          @Override
          public int docID() {
            return docId;
          }

          @Override
          public int index() {
            return index;
          }

          @Override
          public int nextDoc() throws IOException {
            current = docIdMerger.next();
            if (current == null) {
              docId = NO_MORE_DOCS;
              index = NO_MORE_DOCS;
            } else {
              docId = current.mappedDocID;
              ++lastOrd;
              ++index;
            }
            return docId;
          }

          @Override
          public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public long cost() {
            return size;
          }
        };
      }

      @Override
      public float[] vectorValue(int ord) throws IOException {
        if (ord != lastOrd) {
          throw new IllegalStateException(
              "only supports forward iteration with a single iterator: ord="
                  + ord
                  + ", lastOrd="
                  + lastOrd);
        }
        return current.values.vectorValue(current.index());
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return subs.get(0).values.dimension();
      }

      @Override
      public int ordToDoc(int ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public VectorScorer scorer(float[] target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public FloatVectorValues copy() {
        throw new UnsupportedOperationException();
      }
    }

    /**
     * Unified view over several segments containing byte vector values.
     *
     * @lucene.internal
     */
    static class MergedByteVectorValues extends ByteVectorValues {
      private final List<ByteVectorValuesSub> subs;
      private final DocIDMerger<ByteVectorValuesSub> docIdMerger;
      private final int size;

      private int lastOrd = -1;
      private int docId = -1;
      ByteVectorValuesSub current;

      // package-private for testing
      MergedByteVectorValues(List<ByteVectorValuesSub> subs, MergeState mergeState)
          throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (ByteVectorValuesSub sub : subs) {
          totalSize += sub.values.size();
        }
        size = totalSize;
      }

      @Override
      public byte[] vectorValue(int ord) throws IOException {
        if (ord != lastOrd) {
          throw new IllegalStateException(
              "only supports forward iteration: ord=" + ord + ", lastOrd=" + lastOrd);
        }
        return current.values.vectorValue(current.index());
      }

      @Override
      public DocIndexIterator iterator() {
        return new DocIndexIterator() {
          private int index = -1;

          @Override
          public int docID() {
            return docId;
          }

          @Override
          public int index() {
            return index;
          }

          @Override
          public int nextDoc() throws IOException {
            current = docIdMerger.next();
            if (current == null) {
              docId = NO_MORE_DOCS;
              index = NO_MORE_DOCS;
            } else {
              docId = current.mappedDocID;
              ++lastOrd;
              ++index;
            }
            return docId;
          }

          @Override
          public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
          }

          @Override
          public long cost() {
            return size;
          }
        };
      }

      @Override
      public int size() {
        return size;
      }

      @Override
      public int dimension() {
        return subs.get(0).values.dimension();
      }

      @Override
      public int ordToDoc(int ord) {
        throw new UnsupportedOperationException();
      }

      @Override
      public VectorScorer scorer(byte[] target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ByteVectorValues copy() {
        throw new UnsupportedOperationException();
      }
    }
  }
}
