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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Reads vectors from the SPANN (HNSW-IVF) format.
 * 
 * @lucene.experimental
 */
public class Lucene99SpannVectorsReader extends KnnVectorsReader {

    private final KnnVectorsReader centroidDelegate;
    private final Map<String, SpannFieldEntry> fields = new HashMap<>();

    public Lucene99SpannVectorsReader(SegmentReadState state, KnnVectorsFormat centroidFormat) throws IOException {
        this.centroidDelegate = centroidFormat.fieldsReader(state);

        for (FieldInfo fieldInfo : state.fieldInfos) {
            if (fieldInfo.hasVectorValues()) {
                String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
                        fieldInfo.name + ".spam");
                String dataFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
                        fieldInfo.name + ".spad");

                IndexInput metaIn = null;
                IndexInput dataIn = null;
                boolean success = false;

                try {
                    metaIn = state.directory.openInput(metaFileName, state.context);
                    CodecUtil.checkIndexHeader(metaIn, "Lucene99SpannMeta", 0, 0, state.segmentInfo.getId(),
                            state.segmentSuffix);
                    CodecUtil.retrieveChecksum(metaIn);

                    dataIn = state.directory.openInput(dataFileName, state.context);
                    CodecUtil.checkIndexHeader(dataIn, "Lucene99SpannData", 0, 0, state.segmentInfo.getId(),
                            state.segmentSuffix);
                    CodecUtil.retrieveChecksum(dataIn);

                    fields.put(fieldInfo.name, new SpannFieldEntry(metaIn, dataIn));
                    success = true;
                } finally {
                    if (!success) {
                        IOUtils.closeWhileHandlingException(metaIn, dataIn);
                    }
                }
            }
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        centroidDelegate.checkIntegrity();

        for (SpannFieldEntry entry : fields.values()) {
            CodecUtil.checksumEntireFile(entry.metaIn);
            CodecUtil.checksumEntireFile(entry.dataIn);
        }
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return null; // Not supported for random access in this MVP
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return null;
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
        // Impl
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(centroidDelegate);
        for (SpannFieldEntry entry : fields.values()) {
            IOUtils.close(entry.metaIn, entry.dataIn);
        }
    }

    private static class SpannFieldEntry {
        final IndexInput metaIn;
        final IndexInput dataIn;

        SpannFieldEntry(IndexInput metaIn, IndexInput dataIn) {
            this.metaIn = metaIn;
            this.dataIn = dataIn;
        }
    }
}
