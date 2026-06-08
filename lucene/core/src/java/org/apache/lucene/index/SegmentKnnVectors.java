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
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.internal.hppc.LongObjectHashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RefCount;

/**
 * Manages the per-generation {@link KnnVectorsReader}s held by {@link SegmentReader} (one per KNN
 * vector update generation) and keeps track of their reference counting. This is the vector
 * analogue of {@link SegmentDocValues}.
 */
final class SegmentKnnVectors {

  private final LongObjectHashMap<RefCount<KnnVectorsReader>> genVectorsReaders =
      new LongObjectHashMap<>();

  private RefCount<KnnVectorsReader> newVectorsReader(
      SegmentCommitInfo si, Directory dir, final long gen, FieldInfos infos) throws IOException {
    Directory vecDir = dir;
    String segmentSuffix = "";
    if (gen != -1) {
      vecDir = si.info.dir; // gen'd files are written outside CFS, so use SegInfo directory
      segmentSuffix = Long.toString(gen, Character.MAX_RADIX);
    }

    // set SegmentReadState to list only the fields that are relevant to that gen
    SegmentReadState srs =
        new SegmentReadState(vecDir, si.info, infos, IOContext.DEFAULT, segmentSuffix);
    KnnVectorsFormat vectorsFormat = si.info.getCodec().knnVectorsFormat();
    return new RefCount<>(vectorsFormat.fieldsReader(srs)) {
      @SuppressWarnings("synthetic-access")
      @Override
      protected void release() throws IOException {
        object.close();
        synchronized (SegmentKnnVectors.this) {
          genVectorsReaders.remove(gen);
        }
      }
    };
  }

  /** Returns the {@link KnnVectorsReader} for the given generation. */
  synchronized KnnVectorsReader getKnnVectorsReader(
      long gen, SegmentCommitInfo si, Directory dir, FieldInfos infos) throws IOException {
    RefCount<KnnVectorsReader> vecReader = genVectorsReaders.get(gen);
    if (vecReader == null) {
      vecReader = newVectorsReader(si, dir, gen, infos);
      assert vecReader != null;
      genVectorsReaders.put(gen, vecReader);
    } else {
      vecReader.incRef();
    }
    return vecReader.get();
  }

  /** Decrement the reference count of the given {@link KnnVectorsReader} generations. */
  synchronized void decRef(LongArrayList vectorsReadersGens) throws IOException {
    IOUtils.applyToAll(
        vectorsReadersGens.stream().mapToObj(Long::valueOf).toList(),
        gen -> {
          RefCount<KnnVectorsReader> vecReader = genVectorsReaders.get(gen);
          assert vecReader != null : "gen=" + gen;
          vecReader.decRef();
        });
  }
}
