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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores an {@link OrdinalMap} created for a specific {@link IndexReader} ({@code reader}) + {@code
 * field}. Enables re-use of the {@code ordinalMap} once created since creation is costly.
 *
 * <p>Note: It's important that callers confirm the ordinal map is still valid for their cases.
 * Specifically, callers should confirm that the reader used to create the map ({@code reader})
 * matches their use-case.
 */
class StringDocValuesReaderState {

  final IndexReader reader;
  final String field;
  final OrdinalMap ordinalMap;

  StringDocValuesReaderState(IndexReader reader, String field) throws IOException {
    this.reader = reader;
    this.field = field;
    ordinalMap = buildOrdinalMap(reader, field);
  }

  private static OrdinalMap buildOrdinalMap(IndexReader reader, String field) throws IOException {
    List<LeafReaderContext> leaves = reader.leaves();
    int leafCount = leaves.size();

    if (leafCount <= 1) {
      return null;
    }

    SortedSetDocValues[] docValues = new SortedSetDocValues[leafCount];
    for (int i = 0; i < leafCount; i++) {
      LeafReaderContext context = reader.leaves().get(i);
      docValues[i] = DocValues.getSortedSet(context.reader(), field);
    }

    IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
    IndexReader.CacheKey owner = cacheHelper == null ? null : cacheHelper.getKey();

    return OrdinalMap.build(owner, docValues, PackedInts.DEFAULT);
  }

  @Override
  public String toString() {
    return "StringDocValuesReaderState(field=" + field + " reader=" + reader + ")";
  }
}
