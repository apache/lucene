/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Collections;
import java.util.Iterator;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

/**
 * Minimal LeafReader backed only by term vectors for a single field.
 * Used for post-indexing processing such as document graph construction.
 */
public final class TVLeafReader extends LeafReader {

  private final Terms[] termVectors;
  private final FieldInfos fieldInfos;
  private final int maxDoc;
  private final String field;

  /**
   * Creates a reader over the provided term vectors.
   *
   * @param field the field name
   * @param termVectors array of term vectors indexed by docID
   */
  public TVLeafReader(String field, Terms[] termVectors) {
    this.field = field;
    this.termVectors = termVectors;
    this.maxDoc = termVectors.length;

    boolean hasFreqs = false;
    boolean hasPositions = false;
    boolean hasOffsets = false;
    boolean hasPayloads = false;

    for (Terms terms : termVectors) {
      if (terms == null) {
        continue;
      }
      hasFreqs |= terms.hasFreqs();
      hasPositions |= terms.hasPositions();
      hasOffsets |= terms.hasOffsets();
      hasPayloads |= terms.hasPayloads();
    }

    IndexOptions options = IndexOptions.DOCS;
    if (hasFreqs) options = IndexOptions.DOCS_AND_FREQS;
    if (hasPositions) options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    if (hasOffsets) options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

    FieldInfo fieldInfo = new FieldInfo(
        field,
        0,
        true,
        true,
        hasPayloads,
        options,
        DocValuesType.NONE,
        DocValuesSkipIndexType.NONE,
        -1,
        Collections.emptyMap(),
        0, 0, 0, 0,
        null,
        null,
        false,
        false
    );

    this.fieldInfos = new FieldInfos(new FieldInfo[] { fieldInfo });
  }

  @Override
  public Terms terms(String fld) {
    return null;
  }

  @Override
  public TermVectors termVectors() {
    return new TermVectors() {
      @Override
      public Fields get(int docID) {
        if (docID < 0 || docID >= termVectors.length || termVectors[docID] == null) {
          return null;
        }
        return new Fields() {
          @Override
          public Iterator<String> iterator() {
            return Collections.singletonList(field).iterator();
          }

          @Override
          public Terms terms(String fld) {
            return field.equals(fld) ? termVectors[docID] : null;
          }

          @Override
          public int size() {
            return 1;
          }
        };
      }
    };
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) {
    return null;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) {
    return null;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) {
    return null;
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) {
    return null;
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) {
    return null;
  }

  @Override
  public DocValuesSkipper getDocValuesSkipper(String field) {
    return null;
  }

  @Override
  public NumericDocValues getNormValues(String field) {
    return null;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) {
    return null;
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    return null;
  }

  @Override
  public void searchNearestVectors(String field, float[] target, KnnCollector collector, Bits acceptDocs) {}

  @Override
  public void searchNearestVectors(String field, byte[] target, KnnCollector collector, Bits acceptDocs) {}

  @Override
  public PointValues getPointValues(String field) {
    return null;
  }

  @Override
  public StoredFields storedFields() {
    return new StoredFields() {
      @Override
      public void document(int docID, StoredFieldVisitor visitor) {}
    };
  }

  @Override
  public LeafMetaData getMetaData() {
    return new LeafMetaData(Version.LATEST.major, null, null, false);
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  public Bits getLiveDocs() {
    return null;
  }

  @Override
  public int maxDoc() {
    return maxDoc;
  }

  @Override
  public int numDocs() {
    return maxDoc;
  }

  @Override
  protected void doClose() {}

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }

  @Override
  public void checkIntegrity() {}
}