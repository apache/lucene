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
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.MergeState.DocMap;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

final class MultiSorter {

  /**
   * Does a merge sort of the leaves of the incoming reader, returning {@link DocMap} to map each
   * leaf's documents into the merged segment. The documents for each incoming leaf reader must
   * already be sorted by the same sort! Returns null if the merge sort is not needed (segments are
   * already in index sort order).
   */
  static MergeState.DocMap[] sort(Sort sort, List<CodecReader> readers) throws IOException {

    // TODO: optimize if only 1 reader is incoming, though that's a rare case

    SortField[] fields = sort.getSort();
    int leafCount = readers.size();

    // Per-reader parent bitset (used when index sorting with document blocks). Shared across all
    // sort fields; null when the reader does not use blocks. A document's sort value is the value
    // of
    // the parent (last) document of its block.
    final BitSet[] parents = new BitSet[leafCount];
    for (int j = 0; j < leafCount; j++) {
      CodecReader codecReader = readers.get(j);
      FieldInfos fieldInfos = codecReader.getFieldInfos();
      LeafMetaData metaData = codecReader.getMetaData();
      if (metaData.hasBlocks() && fieldInfos.getParentField() != null) {
        NumericDocValues parentDocs = codecReader.getNumericDocValues(fieldInfos.getParentField());
        assert parentDocs != null
            : "parent field: "
                + fieldInfos.getParentField()
                + " must be present if index sorting is used with blocks";
        parents[j] = BitSet.of(parentDocs, codecReader.maxDoc());
      }
      if (metaData.hasBlocks()
          && fieldInfos.getParentField() == null
          && metaData.createdVersionMajor() >= Version.LUCENE_10_0_0.major) {
        throw new CorruptIndexException(
            "parent field is not set but the index has blocks and uses index sorting. indexCreatedVersionMajor: "
                + metaData.createdVersionMajor(),
            "IndexingChain");
      }
    }

    final IndexSorter.ComparableValues[] comparables =
        new IndexSorter.ComparableValues[fields.length];
    final int[] reverseMuls = new int[fields.length];
    for (int i = 0; i < fields.length; i++) {
      IndexSorter sorter = fields[i].getIndexSorter();
      if (sorter == null) {
        throw new IllegalArgumentException(
            "Cannot use sort field " + fields[i] + " for index sorting");
      }
      comparables[i] = sorter.getComparableValues(readers);
      reverseMuls[i] = fields[i].getReverse() ? -1 : 1;
    }

    PriorityQueue<LeafAndDocID> queue =
        PriorityQueue.usingComparator(
            leafCount,
            ((Comparator<LeafAndDocID>)
                    (a, b) -> {
                      for (int i = 0; i < comparables.length; i++) {
                        int cmp = comparables[i].compare(a.readerIndex, b.readerIndex);
                        if (cmp != 0) {
                          return reverseMuls[i] * cmp;
                        }
                      }
                      return 0;
                    })
                .thenComparingInt(ld -> ld.readerIndex)
                .thenComparingInt(ld -> ld.docID));

    PackedLongValues.Builder[] builders = new PackedLongValues.Builder[leafCount];

    for (int i = 0; i < leafCount; i++) {
      CodecReader reader = readers.get(i);
      LeafAndDocID leaf = new LeafAndDocID(i, reader.getLiveDocs(), reader.maxDoc());
      setComparableValues(comparables, parents[i], i, leaf.docID);
      queue.add(leaf);
      builders[i] = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
    }

    // merge sort:
    int mappedDocID = 0;
    int lastReaderIndex = 0;
    boolean isSorted = true;
    while (queue.size() != 0) {
      LeafAndDocID top = queue.top();
      if (lastReaderIndex > top.readerIndex) {
        // merge sort is needed
        isSorted = false;
      }
      lastReaderIndex = top.readerIndex;
      builders[top.readerIndex].add(mappedDocID);
      if (top.liveDocs == null || top.liveDocs.get(top.docID)) {
        mappedDocID++;
      }
      top.docID++;
      if (top.docID < top.maxDoc) {
        setComparableValues(comparables, parents[top.readerIndex], top.readerIndex, top.docID);
        queue.updateTop();
      } else {
        queue.pop();
      }
    }
    if (isSorted) {
      return null;
    }

    MergeState.DocMap[] docMaps = new MergeState.DocMap[leafCount];
    for (int i = 0; i < leafCount; i++) {
      final PackedLongValues remapped = builders[i].build();
      final Bits liveDocs = readers.get(i).getLiveDocs();
      docMaps[i] =
          docID -> {
            if (liveDocs == null || liveDocs.get(docID)) {
              return (int) remapped.get(docID);
            } else {
              return -1;
            }
          };
    }

    return docMaps;
  }

  /**
   * Caches, for every sort field, the value of the given document of the given segment so that the
   * priority queue can compare it. When the segment uses document blocks, the value of the parent
   * (last) document of {@code docID}'s block is used instead.
   */
  private static void setComparableValues(
      IndexSorter.ComparableValues[] comparables, BitSet parents, int readerIndex, int docID)
      throws IOException {
    final int effectiveDocID = parents == null ? docID : parents.nextSetBit(docID);
    for (IndexSorter.ComparableValues comparable : comparables) {
      comparable.setTopValue(readerIndex, effectiveDocID);
    }
  }

  private static class LeafAndDocID {
    final int readerIndex;
    final Bits liveDocs;
    final int maxDoc;
    int docID;

    public LeafAndDocID(int readerIndex, Bits liveDocs, int maxDoc) {
      this.readerIndex = readerIndex;
      this.liveDocs = liveDocs;
      this.maxDoc = maxDoc;
    }
  }
}
