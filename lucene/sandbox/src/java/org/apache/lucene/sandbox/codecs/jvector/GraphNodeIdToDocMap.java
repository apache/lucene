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

package org.apache.lucene.sandbox.codecs.jvector;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * This class represents the mapping from the Lucene document IDs to the jVector ordinals. This
 * mapping is necessary because the jVector ordinals can be different from the Lucene document IDs
 * and when lucene documentIDs change after a merge, we need to update this mapping to reflect the
 * new document IDs. This requires us to know the previous mapping from the previous merge and the
 * new mapping from the current merge.
 *
 * <p>Which means that we also need to persist this mapping to disk to be available across merges.
 */
public class GraphNodeIdToDocMap {
  private static final int VERSION = 1;
  private final int[] graphNodeIdsToDocIds;
  private final int[] docIdsToGraphNodeIds;

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
    Arrays.fill(docIdsToGraphNodeIds, -1);
    for (int ord = 0; ord < size; ord++) {
      final int docId = in.readVInt();
      graphNodeIdsToDocIds[ord] = docId;
      docIdsToGraphNodeIds[docId] = ord;
    }
  }

  public GraphNodeIdToDocMap(DocsWithFieldSet docs) {
    this.graphNodeIdsToDocIds = new int[docs.cardinality()];

    int ord = 0;
    int maxDocId = -1;
    final var docsIterator = docs.iterator();
    try {
      for (int docId = docsIterator.nextDoc();
          docId != NO_MORE_DOCS;
          docId = docsIterator.nextDoc()) {
        graphNodeIdsToDocIds[ord++] = docId;
        if (docId > maxDocId) {
          maxDocId = docId;
        }
      }
    } catch (IOException e) {
      // This should never happen; docsIterator should be FixedBitSet or DocSetIterator.all()
      throw new UncheckedIOException(e);
    }

    this.docIdsToGraphNodeIds = new int[maxDocId + 1];
    Arrays.fill(docIdsToGraphNodeIds, -1);
    for (ord = 0; ord < graphNodeIdsToDocIds.length; ++ord) {
      docIdsToGraphNodeIds[graphNodeIdsToDocIds[ord]] = ord;
    }
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
   *     <p>NOTE: This method is useful when, for example, we want to remap acceptedDocs bitmap from
   *     Lucene to jVector ordinal bitmap filter
   */
  public int getLuceneDocId(int graphNodeId) {
    return graphNodeIdsToDocIds[graphNodeId];
  }

  public int getMaxDocId() {
    return docIdsToGraphNodeIds.length - 1;
  }

  public int getMaxOrd() {
    return graphNodeIdsToDocIds.length - 1;
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

  public DocIndexIterator iterator() {
    return new DocIndexIterator() {
      int docId = -1;

      @Override
      public int index() {
        return docIdsToGraphNodeIds[docId];
      }

      @Override
      public int docID() {
        return docId;
      }

      @Override
      public int nextDoc() throws IOException {
        while (docId < docIdsToGraphNodeIds.length - 1) {
          ++docId;
          final int ord = docIdsToGraphNodeIds[docId];
          if (ord >= 0) {
            return docId;
          }
        }
        return docId = NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) throws IOException {
        if (target <= docId) {
          throw new IllegalArgumentException();
        } else if (target >= docIdsToGraphNodeIds.length) {
          return docId = NO_MORE_DOCS;
        }

        docId = target - 1;
        return nextDoc();
      }

      @Override
      public long cost() {
        return graphNodeIdsToDocIds.length;
      }
    };
  }
}
