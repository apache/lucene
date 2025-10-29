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

package org.opensearch.knn.index.codec.jvector;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class represents the mapping from the Lucene document IDs to the jVector ordinals.
 * This mapping is necessary because the jVector ordinals can be different from the Lucene document IDs and when lucene documentIDs change after a merge,
 * we need to update this mapping to reflect the new document IDs.
 * This requires us to know the previous mapping from the previous merge and the new mapping from the current merge.
 * <p>
 * Which means that we also need to persist this mapping to disk to be available across merges.
 */
@Log4j2
public class GraphNodeIdToDocMap {
    private static final int VERSION = 1;
    private int[] graphNodeIdsToDocIds;
    private int[] docIdsToGraphNodeIds;

    /**
     * Constructor that reads the mapping from the index input
     *
     * @param in The index input
     * @throws IOException if an I/O error occurs
     */
    public GraphNodeIdToDocMap(IndexInput in) throws IOException {
        final int version = in.readInt(); // Read the version
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        int size = in.readVInt();
        int maxDocId = in.readVInt();

        graphNodeIdsToDocIds = new int[size];
        docIdsToGraphNodeIds = new int[maxDocId];
        for (int ord = 0; ord < size; ord++) {
            final int docId = in.readVInt();
            graphNodeIdsToDocIds[ord] = docId;
            docIdsToGraphNodeIds[docId] = ord;
        }
    }

    /**
     * Constructor that creates a new mapping between ordinals and docIds
     *
     * @param graphNodeIdsToDocIds The mapping from ordinals to docIds
     */
    public GraphNodeIdToDocMap(int[] graphNodeIdsToDocIds) {
        if (graphNodeIdsToDocIds.length == 0) {
            this.graphNodeIdsToDocIds = new int[0];
            this.docIdsToGraphNodeIds = new int[0];
            return;
        }
        this.graphNodeIdsToDocIds = new int[graphNodeIdsToDocIds.length];
        System.arraycopy(graphNodeIdsToDocIds, 0, this.graphNodeIdsToDocIds, 0, graphNodeIdsToDocIds.length);
        final int maxDocId = Arrays.stream(graphNodeIdsToDocIds).max().getAsInt();
        final int maxDocs = maxDocId + 1;
        // We are going to assume that the number of ordinals is roughly the same as the number of documents in the segment, therefore,
        // the mapping will not be sparse.
        if (maxDocs < graphNodeIdsToDocIds.length) {
            throw new IllegalStateException("Max docs " + maxDocs + " is less than the number of ordinals " + graphNodeIdsToDocIds.length);
        }
        if (maxDocId > graphNodeIdsToDocIds.length) {
            log.warn(
                "Max doc id {} is greater than the number of ordinals {}, this implies a lot of deleted documents. Or that some documents are missing vectors. Wasting a lot of memory",
                maxDocId,
                graphNodeIdsToDocIds.length
            );
        }
        this.docIdsToGraphNodeIds = new int[maxDocs];
        Arrays.fill(this.docIdsToGraphNodeIds, -1); // -1 means no mapping to ordinal
        for (int ord = 0; ord < graphNodeIdsToDocIds.length; ord++) {
            this.docIdsToGraphNodeIds[graphNodeIdsToDocIds[ord]] = ord;
        }
    }

    /**
     * Updates the mapping from the Lucene document IDs to the jVector ordinals based on the sort operation. (during flush)
     *
     * @param sortMap The sort map
     */
    public void update(Sorter.DocMap sortMap) {
        final int[] newGraphNodeIdsToDocIds = new int[graphNodeIdsToDocIds.length];
        final int maxNewDocId = Arrays.stream(graphNodeIdsToDocIds).map(sortMap::oldToNew).max().getAsInt();
        final int maxDocs = maxNewDocId + 1;
        if (maxDocs < graphNodeIdsToDocIds.length) {
            throw new IllegalStateException("Max docs " + maxDocs + " is less than the number of ordinals " + graphNodeIdsToDocIds.length);
        }
        final int[] newDocIdsToOrdinals = new int[maxDocs];
        Arrays.fill(newDocIdsToOrdinals, -1);
        for (int oldDocId = 0; oldDocId < docIdsToGraphNodeIds.length; oldDocId++) {
            if (docIdsToGraphNodeIds[oldDocId] == -1) {
                continue;
            }
            final int newDocId = sortMap.oldToNew(oldDocId);
            final int oldOrd = docIdsToGraphNodeIds[oldDocId];
            newDocIdsToOrdinals[newDocId] = oldOrd;
            newGraphNodeIdsToDocIds[oldOrd] = newDocId;
        }
        this.docIdsToGraphNodeIds = newDocIdsToOrdinals;
        this.graphNodeIdsToDocIds = newGraphNodeIdsToDocIds;
    }

    /**
     * Returns the jVector node id for the given Lucene document ID
     *
     * @param luceneDocId The Lucene document ID
     * @return The jVector ordinal
     */
    public int getJVectorNodeId(int luceneDocId) {
        return docIdsToGraphNodeIds[luceneDocId];
    }

    /**
     * Returns the Lucene document ID for the given jVector node id
     *
     * @param graphNodeId The jVector ordinal
     * @return The Lucene document ID
     * <p>
     * NOTE: This method is useful when, for example, we want to remap acceptedDocs bitmap from Lucene to jVector ordinal bitmap filter
     */
    public int getLuceneDocId(int graphNodeId) {
        return graphNodeIdsToDocIds[graphNodeId];
    }

    /**
     * Writes the mapping to the index output
     *
     * @param out The index output
     * @throws IOException if an I/O error occurs
     */
    public void toOutput(IndexOutput out) throws IOException {
        out.writeInt(VERSION);
        out.writeVInt(graphNodeIdsToDocIds.length);
        out.writeVInt(docIdsToGraphNodeIds.length);
        for (int ord = 0; ord < graphNodeIdsToDocIds.length; ord++) {
            out.writeVInt(graphNodeIdsToDocIds[ord]);
        }
    }
}
