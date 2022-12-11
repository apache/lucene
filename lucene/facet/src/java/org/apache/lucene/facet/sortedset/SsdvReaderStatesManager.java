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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.IndexSearcher;

/**
 * This class demonstrates how Lucene could help a user build SSDV ordinal maps eagerly, on refresh.
 * This class offers:
 *
 * <p>1. A centralized store of SSDV reader states.
 *
 * <p>2. A method to rebuild all reader states the user needs.
 */
public class SsdvReaderStatesManager {
  /**
   * The only attribute is a mapping of SSDV fields to reader states. Each reader state will be a
   * {@link DefaultSortedSetDocValuesReaderState} with an ordinal map for one of the fields.
   */
  private Map<String, SortedSetDocValuesReaderState> fieldToReaderState;

  /** No arguments in the constructor. */
  SsdvReaderStatesManager() {}

  /**
   * The application code would have a set of fields the user wants to facet on. They pass this set
   * here once, on application start-up.
   */
  public void registerFields(Set<String> fields) {
    fields.forEach(field -> fieldToReaderState.put(field, null));
  }

  /**
   * Here we create corresponding reader states for all the fields. Each time the user does a
   * refresh, they would also call this method, which ensures they will be using up-to-date ordinal
   * maps until the next refresh.
   */
  public void loadReaderStates(IndexSearcher searcher, FacetsConfig facetsConfig) {
    fieldToReaderState.replaceAll(
        (field, readerState) -> {
          try {
            return new DefaultSortedSetDocValuesReaderState(
                searcher.getIndexReader(), field, facetsConfig);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  /**
   * When the user wants to facet on a field, they call getReaderState on that field and pass the
   * resulting reader state to the {@link SortedSetDocValuesFacetCounts} constructor.
   */
  public SortedSetDocValuesReaderState getReaderState(String field) {
    return fieldToReaderState.get(field);
  }
}
