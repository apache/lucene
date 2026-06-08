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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.Directory;

/**
 * Encapsulates multiple per-generation {@link KnnVectorsReader}s as one reader when there are KNN
 * vector updates. This is the vector analogue of {@link SegmentDocValuesProducer}.
 *
 * <p>Fields with no vector updates (vectorGen == -1) are served from the shared, immutable base
 * reader held by {@link SegmentCoreReaders}; that base reader is never closed by this producer (its
 * lifecycle is owned by the core). Fields with updates are served from per-gen readers tracked by
 * {@link SegmentKnnVectors} via reference counting.
 */
class SegmentVectorsProducer extends KnnVectorsReader {

  private final FieldInfos fieldInfos;
  final IntObjectHashMap<KnnVectorsReader> readersByField = new IntObjectHashMap<>();
  // distinct gen readers (excluding the base core reader) for checkIntegrity
  final Set<KnnVectorsReader> genReaders =
      Collections.newSetFromMap(new IdentityHashMap<KnnVectorsReader, Boolean>());
  final LongArrayList vectorGens = new LongArrayList();

  /**
   * Creates a new producer that handles updated KNN vector fields.
   *
   * @param si commit point
   * @param dir directory
   * @param baseCoreReader the shared base (gen == -1) reader from {@link SegmentCoreReaders}; may
   *     be {@code null} if the core has no vector fields
   * @param allInfos all fieldinfos including updated ones
   * @param segKnnVectors per-gen reader cache
   */
  SegmentVectorsProducer(
      SegmentCommitInfo si,
      Directory dir,
      KnnVectorsReader baseCoreReader,
      FieldInfos allInfos,
      SegmentKnnVectors segKnnVectors)
      throws IOException {
    this.fieldInfos = allInfos;
    try {
      for (FieldInfo fi : allInfos) {
        if (fi.hasVectorValues() == false) {
          continue;
        }
        long vectorGen = fi.getVectorGen();
        if (vectorGen == -1) {
          // served from the shared base core reader; not tracked here
          readersByField.put(fi.number, baseCoreReader);
        } else {
          assert !vectorGens.contains(vectorGen);
          // otherwise, reader sees only the one fieldinfo it wrote
          final KnnVectorsReader vecReader =
              segKnnVectors.getKnnVectorsReader(
                  vectorGen, si, dir, new FieldInfos(new FieldInfo[] {fi}));
          vectorGens.add(vectorGen);
          genReaders.add(vecReader);
          readersByField.put(fi.number, vecReader);
        }
      }
    } catch (Throwable t) {
      try {
        segKnnVectors.decRef(vectorGens);
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  private KnnVectorsReader readerForField(String field) {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    assert fi != null && fi.hasVectorValues();
    KnnVectorsReader reader = readersByField.get(fi.number);
    assert reader != null : "no vectors reader for field: " + field;
    return reader;
  }

  @Override
  public KnnVectorsReader unwrapReaderForField(String field) {
    return readerForField(field).unwrapReaderForField(field);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return readerForField(field).getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return readerForField(field).getByteVectorValues(field);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    readerForField(field).search(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    readerForField(field).search(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void checkIntegrity() throws IOException {
    // the base core reader is checked separately by SegmentCoreReaders; only check the gen readers
    for (KnnVectorsReader reader : genReaders) {
      reader.checkIntegrity();
    }
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException(); // there is separate ref tracking
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(genReaders=" + genReaders.size() + ")";
  }
}
