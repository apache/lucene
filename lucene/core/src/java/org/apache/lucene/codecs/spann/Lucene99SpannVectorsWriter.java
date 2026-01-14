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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Writes vectors in the SPANN (HNSW-IVF) format.
 * <p>
 * <li><b>Centroids</b>: Computed via K-Means and indexed into the underlying
 * HNSW delegate.</li>
 * <li><b>Data</b>: Vectors are assigned to their nearest centroid and written
 * sequentially to a .spad file.</li>
 */
public class Lucene99SpannVectorsWriter extends KnnVectorsWriter {

    private final SegmentWriteState state;
    private final KnnVectorsWriter centroidDelegate;
    private final Map<String, SpannFieldVectorsWriter> fieldWriters = new HashMap<>();

    public Lucene99SpannVectorsWriter(SegmentWriteState state, KnnVectorsFormat centroidFormat) throws IOException {
        this.state = state;
        this.centroidDelegate = centroidFormat.fieldsWriter(state);
    }

    @Override
    public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        SpannFieldVectorsWriter writer = new SpannFieldVectorsWriter(fieldInfo);
        fieldWriters.put(fieldInfo.name, writer);
        return writer;
    }

    @Override
    public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {

        for (Map.Entry<String, SpannFieldVectorsWriter> entry : fieldWriters.entrySet()) {
            String fieldName = entry.getKey();
            SpannFieldVectorsWriter writer = entry.getValue();
            FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldName);

            if (writer.getVectors().isEmpty()) {
                continue;
            }

            float[][] vectorArray = writer.getVectors().toArray(new float[0][]);

            // target partitions: 100 * clusters heuristic
            // TODO: derive dynamically from vector count (e.g. Math.sqrt(N))
            int numPartitions = Math.min(vectorArray.length, 100);

            float[][] centroids = SpannKMeans.cluster(
                    vectorArray,
                    numPartitions,
                    fieldInfo.getVectorSimilarityFunction(),
                    10);

            KnnFieldVectorsWriter<?> centroidWriter = centroidDelegate.addField(fieldInfo);

            // Map partitionId to docId in the centroid index
            for (int partitionId = 0; partitionId < centroids.length; partitionId++) {
                centroidWriter.addValue(partitionId, centroids[partitionId]);
            }

            @SuppressWarnings("unchecked")
            List<float[]>[] partitions = new List[centroids.length];
            for (int i = 0; i < centroids.length; i++) {
                partitions[i] = new ArrayList<>();
            }

            VectorSimilarityFunction simFunc = fieldInfo.getVectorSimilarityFunction();
            for (float[] vector : writer.getVectors()) {
                int bestCentroid = 0;
                float bestScore = Float.NEGATIVE_INFINITY;

                for (int c = 0; c < centroids.length; c++) {
                    float score = simFunc.compare(vector, centroids[c]);
                    if (score > bestScore) {
                        bestScore = score;
                        bestCentroid = c;
                    }
                }
                partitions[bestCentroid].add(vector);
            }

            String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
                    fieldName + ".spad");
            String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
                    fieldName + ".spam");

            try (IndexOutput dataOut = state.directory.createOutput(dataFileName, state.context);
                    IndexOutput metaOut = state.directory.createOutput(metaFileName, state.context)) {

                CodecUtil.writeIndexHeader(dataOut, "Lucene99SpannData", 0, state.segmentInfo.getId(),
                        state.segmentSuffix);
                CodecUtil.writeIndexHeader(metaOut, "Lucene99SpannMeta", 0, state.segmentInfo.getId(),
                        state.segmentSuffix);

                for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
                    List<float[]> clusterVectors = partitions[partitionId];
                    long startOffset = dataOut.getFilePointer();

                    for (float[] v : clusterVectors) {
                        for (float f : v) {
                            dataOut.writeInt(Float.floatToIntBits(f));
                        }
                    }

                    long lengthBytes = dataOut.getFilePointer() - startOffset;

                    metaOut.writeVInt(partitionId);
                    metaOut.writeVLong(startOffset);
                    metaOut.writeVLong(lengthBytes);
                }

                CodecUtil.writeFooter(dataOut);
                CodecUtil.writeFooter(metaOut);
            }
        }

        centroidDelegate.flush(maxDoc, sortMap);
    }

    @Override
    public void finish() throws IOException {
        centroidDelegate.finish();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(centroidDelegate);
    }
}
