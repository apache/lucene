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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link ParallelTaxonomyArrays} that are initialized from the taxonomy index.
 *
 * @lucene.experimental
 */
class TaxonomyIndexArrays extends ParallelTaxonomyArrays implements Accountable {
  private static final int CHUNK_SIZE_BITS = 13;
  static final int CHUNK_SIZE = 1 << CHUNK_SIZE_BITS;
  private static final int CHUNK_MASK = CHUNK_SIZE - 1;

  private final ChunkedIntArray parents;

  // the following two arrays are lazily initialized. note that we only keep a
  // single boolean member as volatile, instead of declaring the arrays
  // volatile. the code guarantees that only after the boolean is set to true,
  // the arrays are returned.
  private volatile boolean initializedChildren = false;
  private ChunkedIntArray children, siblings;

  static class ChunkedIntArray extends ParallelTaxonomyArrays.IntArray {
    final int[][] values;

    private ChunkedIntArray(int[][] values) {
      this.values = values;
    }

    @Override
    public int get(int i) {
      return values[i >> CHUNK_SIZE_BITS][i & CHUNK_MASK];
    }

    public void set(int i, int val) {
      values[i >> CHUNK_SIZE_BITS][i & CHUNK_MASK] = val;
    }

    @Override
    public int length() {
      return ((values.length - 1) << CHUNK_SIZE_BITS) + values[values.length - 1].length;
    }
  }

  /** Used by {@link #add(int, int)} after the array grew. Also, used for testing. */
  TaxonomyIndexArrays(int[][] parents) {
    this.parents = new ChunkedIntArray(parents);
  }

  public TaxonomyIndexArrays(IndexReader reader) throws IOException {
    int[][] parentArray = allocateChunkedArray(reader.maxDoc(), 0);
    assert parentArray.length > 0;
    if (parentArray[0].length > 0) {
      initParents(parentArray, reader, 0);
      parentArray[0][0] = TaxonomyReader.INVALID_ORDINAL;
    }
    parents = new ChunkedIntArray(parentArray);
  }

  public TaxonomyIndexArrays(IndexReader reader, TaxonomyIndexArrays copyFrom) throws IOException {
    assert copyFrom != null;

    // note that copyParents.length may be equal to reader.maxDoc(). this is not a bug
    // it may be caused if e.g. the taxonomy segments were merged, and so an updated
    // NRT reader was obtained, even though nothing was changed. this is not very likely
    // to happen.
    int[][] parentArray = allocateChunkedArray(reader.maxDoc(), copyFrom.parents.values.length - 1);
    assert parentArray.length > 0;

    copyChunkedArray(copyFrom.parents.values, parentArray);
    initParents(parentArray, reader, copyFrom.parents.length());
    parents = new ChunkedIntArray(parentArray);
    if (copyFrom.initializedChildren) {
      initChildrenSiblings(copyFrom);
    }
  }

  private static int[][] allocateChunkedArray(int size, int startFrom) {
    int chunkCount = (size >> CHUNK_SIZE_BITS) + 1;
    int[][] array = new int[chunkCount][];
    for (int i = startFrom; i < chunkCount - 1; i++) {
      array[i] = new int[CHUNK_SIZE];
    }
    array[chunkCount - 1] = new int[size & CHUNK_MASK];
    return array;
  }

  private static void copyChunkedArray(int[][] oldArray, int[][] newArray) {
    // Copy all but the last (maybe partial) chunk from the old array
    if (oldArray.length > 1) {
      System.arraycopy(oldArray, 0, newArray, 0, oldArray.length - 1);
    }
    int[] lastCopyChunk = oldArray[oldArray.length - 1];
    System.arraycopy(lastCopyChunk, 0, newArray[oldArray.length - 1], 0, lastCopyChunk.length);
  }

  private synchronized void initChildrenSiblings(TaxonomyIndexArrays copyFrom) {
    if (!initializedChildren) { // must do this check !
      int startFrom;
      if (copyFrom == null) {
        startFrom = 0;
      } else {
        startFrom = copyFrom.parents.values.length - 1;
      }
      int[][] childrenArray = allocateChunkedArray(parents.length(), startFrom);
      int[][] siblingsArray = allocateChunkedArray(parents.length(), startFrom);
      // Rely on these arrays being copied by reference, since we may modify them below
      children = new ChunkedIntArray(childrenArray);
      siblings = new ChunkedIntArray(siblingsArray);
      if (copyFrom != null) {
        // called from the ctor, after we know copyFrom has initialized children/siblings
        copyChunkedArray(copyFrom.children.values, childrenArray);
        copyChunkedArray(copyFrom.siblings.values, siblingsArray);
        computeChildrenSiblings(copyFrom.parents.length());
      } else {
        computeChildrenSiblings(0);
      }
      initializedChildren = true;
    }
  }

  private void computeChildrenSiblings(int first) {
    // reset the youngest child of all ordinals. while this should be done only
    // for the leaves, we don't know up front which are the leaves, so we reset
    // all of them.
    int length = parents.length();
    for (int i = first; i < length; i++) {
      children.set(i, TaxonomyReader.INVALID_ORDINAL);
    }

    // the root category has no parent, and therefore no siblings
    if (first == 0) {
      first = 1;
      siblings.set(0, TaxonomyReader.INVALID_ORDINAL);
    }

    int firstChunkStart = first - (first & CHUNK_MASK);
    IntHashSet reallocatedChildChunks = new IntHashSet();
    for (int i = first; i < length; i++) {
      int parent = parents.get(i);
      // The existing youngest child of the parent is the next older sibling of i.
      // note that parents[i] is always < i, so the right-hand-side of
      // the following line is already set when we get here
      siblings.set(i, children.get(parent));
      // The new youngest child of the parent is i.
      if (parent < firstChunkStart) {
        int chunkIdx = parent >> CHUNK_SIZE_BITS;
        if (reallocatedChildChunks.contains(chunkIdx) == false) {
          reallocatedChildChunks.add(chunkIdx);
          int[] oldChildren = children.values[chunkIdx];
          children.values[chunkIdx] = new int[CHUNK_SIZE];
          System.arraycopy(oldChildren, 0, children.values[chunkIdx], 0, oldChildren.length);
        }
      }
      children.set(parent, i);
    }
  }

  // Read the parents of the new categories
  private void initParents(int[][] parentsArray, IndexReader reader, int first) throws IOException {
    if (reader.maxDoc() == first) {
      return;
    }

    if (getMajorVersion(reader) <= 8) {
      loadParentUsingTermPosition(parentsArray, reader, first);
      return;
    }

    for (LeafReaderContext leafContext : reader.leaves()) {
      int leafDocNum = leafContext.reader().maxDoc();
      if (leafContext.docBase + leafDocNum <= first) {
        // skip this leaf if it does not contain new categories
        continue;
      }
      NumericDocValues parentValues =
          leafContext.reader().getNumericDocValues(Consts.FIELD_PARENT_ORDINAL_NDV);
      if (parentValues == null) {
        throw new CorruptIndexException(
            "Parent data field " + Consts.FIELD_PARENT_ORDINAL_NDV + " does not exist",
            leafContext.reader().toString());
      }

      for (int doc = Math.max(first - leafContext.docBase, 0); doc < leafDocNum; doc++) {
        if (parentValues.advanceExact(doc) == false) {
          throw new CorruptIndexException(
              "Missing parent data for category " + (doc + leafContext.docBase), reader.toString());
        }
        int pos = doc + leafContext.docBase;
        parentsArray[pos >> CHUNK_SIZE_BITS][pos & CHUNK_MASK] =
            Math.toIntExact(parentValues.longValue());
      }
    }
  }

  private static int getMajorVersion(IndexReader reader) {
    assert reader.leaves().size() > 0;
    return reader.leaves().get(0).reader().getMetaData().getCreatedVersionMajor();
  }

  /**
   * Try loading the old way of storing parent ordinal first, return true if the parent array is
   * loaded Or false if not, and we will try loading using NumericDocValues
   */
  // TODO: Remove in Lucene 10, this is only for back-compatibility
  private void loadParentUsingTermPosition(int[][] parentsArray, IndexReader reader, int first)
      throws IOException {
    // it's ok to use MultiTerms because we only iterate on one posting list.
    // breaking it to loop over the leaves() only complicates code for no
    // apparent gain.
    PostingsEnum positions =
        MultiTerms.getTermPostingsEnum(
            reader, Consts.FIELD_PAYLOADS, Consts.PAYLOAD_PARENT_BYTES_REF, PostingsEnum.PAYLOADS);

    // shouldn't really happen, if it does, something's wrong
    if (positions == null || positions.advance(first) == DocIdSetIterator.NO_MORE_DOCS) {
      throw new CorruptIndexException(
          "[Lucene 8] Missing parent data for category " + first, reader.toString());
    }

    int num = reader.maxDoc();
    for (int i = first; i < num; i++) {
      if (positions.docID() == i) {
        if (positions.freq() == 0) { // shouldn't happen
          throw new CorruptIndexException(
              "[Lucene 8] Missing parent data for category " + i, reader.toString());
        }

        parentsArray[i >> CHUNK_SIZE_BITS][i & CHUNK_MASK] = positions.nextPosition();

        if (positions.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
          if (i + 1 < num) {
            throw new CorruptIndexException(
                "[Lucene 8] Missing parent data for category " + (i + 1), reader.toString());
          }
          break;
        }
      } else { // this shouldn't happen
        throw new CorruptIndexException(
            "[Lucene 8] Missing parent data for category " + i, reader.toString());
      }
    }
  }

  /**
   * Adds the given ordinal/parent info and returns either a new instance if the underlying array
   * had to grow, or this instance otherwise.
   *
   * <p><b>NOTE:</b> you should call this method from a thread-safe code.
   */
  TaxonomyIndexArrays add(int ordinal, int parentOrdinal) {
    if (ordinal >= parents.length()) {
      int[][] newParents =
          allocateChunkedArray(
              ArrayUtil.oversize(ordinal + 1, Integer.BYTES), parents.values.length - 1);
      copyChunkedArray(parents.values, newParents);
      newParents[ordinal >> CHUNK_SIZE_BITS][ordinal & CHUNK_MASK] = parentOrdinal;
      return new TaxonomyIndexArrays(newParents);
    }
    parents.set(ordinal, parentOrdinal);
    return this;
  }

  /**
   * Returns the parents array, where {@code parents[i]} denotes the parent of category ordinal
   * {@code i}.
   */
  @Override
  public ChunkedIntArray parents() {
    return parents;
  }

  /**
   * Returns the children array, where {@code children[i]} denotes the youngest child of category
   * ordinal {@code i}. The youngest child is defined as the category that was added last to the
   * taxonomy as an immediate child of {@code i}.
   */
  @Override
  public ChunkedIntArray children() {
    if (!initializedChildren) {
      initChildrenSiblings(null);
    }

    // the array is guaranteed to be populated
    return children;
  }

  /**
   * Returns the siblings array, where {@code siblings[i]} denotes the sibling of category ordinal
   * {@code i}. The sibling is defined as the previous youngest child of {@code parents[i]}.
   */
  @Override
  public ChunkedIntArray siblings() {
    if (!initializedChildren) {
      initChildrenSiblings(null);
    }

    // the array is guaranteed to be populated
    return siblings;
  }

  @Override
  public synchronized long ramBytesUsed() {
    long ramBytesUsed =
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF + 1;
    ramBytesUsed += RamUsageEstimator.shallowSizeOf(parents);
    if (children != null) {
      ramBytesUsed += RamUsageEstimator.shallowSizeOf(children);
    }
    if (siblings != null) {
      ramBytesUsed += RamUsageEstimator.shallowSizeOf(siblings);
    }
    return ramBytesUsed;
  }

  @Override
  public synchronized Collection<Accountable> getChildResources() {
    final List<Accountable> resources = new ArrayList<>();
    resources.add(
        Accountables.namedAccountable("parents", RamUsageEstimator.shallowSizeOf(parents)));
    if (children != null) {
      resources.add(
          Accountables.namedAccountable("children", RamUsageEstimator.shallowSizeOf(children)));
    }
    if (siblings != null) {
      resources.add(
          Accountables.namedAccountable("siblings", RamUsageEstimator.shallowSizeOf(siblings)));
    }
    return Collections.unmodifiableList(resources);
  }
}
