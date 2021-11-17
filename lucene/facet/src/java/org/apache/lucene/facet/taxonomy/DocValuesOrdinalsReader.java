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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/**
 * Decodes ordinals previously indexed into a BinaryDocValues field
 *
 * @deprecated Custom binary encodings for taxonomy ordinals are no longer supported starting with
 *     Lucene 9
 */
@Deprecated
public class DocValuesOrdinalsReader extends OrdinalsReader {
  private final String field;

  /** Default constructor. */
  public DocValuesOrdinalsReader() {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  /** Create this, with the specified indexed field name. */
  public DocValuesOrdinalsReader(String field) {
    this.field = field;
  }

  @Override
  public OrdinalsSegmentReader getReader(LeafReaderContext context) throws IOException {
    SortedNumericDocValues dv0 =
        FacetUtils.loadOrdinalValues(context.reader(), field, this::decode);
    if (dv0 == null) {
      dv0 = DocValues.emptySortedNumeric();
    }
    final SortedNumericDocValues dv = dv0;

    return new OrdinalsSegmentReader() {

      private int lastDocID;

      @Override
      public void get(int docID, IntsRef ordinals) throws IOException {
        if (docID < lastDocID) {
          throw new AssertionError(
              "docs out of order: lastDocID=" + lastDocID + " vs docID=" + docID);
        }
        lastDocID = docID;

        ordinals.offset = 0;
        ordinals.length = 0;

        if (dv.advanceExact(docID)) {
          int count = dv.docValueCount();
          if (ordinals.ints.length < count) {
            ordinals.ints = ArrayUtil.grow(ordinals.ints, count);
          }

          for (int i = 0; i < count; i++) {
            ordinals.ints[ordinals.length] = (int) dv.nextValue();
            ordinals.length++;
          }
        }
      }
    };
  }

  @Override
  public String getIndexFieldName() {
    return field;
  }

  /**
   * Subclass and override if you change the encoding. The method is marked 'public' to allow
   * decoding of binary payload containing ordinals without instantiating an {@link
   * org.apache.lucene.facet.taxonomy.OrdinalsReader.OrdinalsSegmentReader}.
   *
   * <p>This takes care of use cases where an application instantiates {@link
   * org.apache.lucene.index.BinaryDocValues} reader for a facet field outside this class, reads the
   * binary payload for a document and decodes the ordinals in the payload.
   *
   * @param buf binary payload containing encoded ordinals
   * @param ordinals buffer for decoded ordinals
   */
  public void decode(BytesRef buf, IntsRef ordinals) {
    BackCompatSortedNumericDocValues.loadValues(buf, ordinals);
  }
}
