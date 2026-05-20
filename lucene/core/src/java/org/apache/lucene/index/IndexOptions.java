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

import org.apache.lucene.search.PhraseQuery;

/**
 * Controls how much information is stored in the postings lists.
 *
 * @lucene.experimental
 */
public enum IndexOptions {
  // NOTE: order is important here; enum ordinals can be used to compare options.

  /** Not indexed */
  NONE,
  /**
   * Only documents are indexed: term frequencies and positions are omitted. Phrase and other
   * positional queries on the field will throw an exception, and scoring will behave as if any term
   * in the document appears only once.
   */
  DOCS,
  /**
   * Only documents and term frequencies are indexed: positions are omitted. This enables normal
   * scoring, except {@link PhraseQuery} and other positional queries will throw an exception.
   */
  DOCS_AND_FREQS,
  /**
   * Indexes documents, frequencies and positions. This is a typical default for full-text search:
   * full scoring is enabled and positional queries are supported.
   */
  DOCS_AND_FREQS_AND_POSITIONS,
  /**
   * Indexes documents, frequencies, positions and offsets. Character offsets are encoded alongside
   * the positions.
   */
  DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
  /**
   * Like DOCS_AND_FREQS except the "custom freqs" are treated as scores and do not represent term
   * counts. Each term can only occur once per document, so its frequency in that sense is always 1.
   */
  DOCS_AND_CUSTOM_FREQS;

  /**
   * Return true if the index format encoding this IndexOptions includes the bits required for the
   * other index format. This function provides a partial ordering of the options. The options can't
   * be totally ordered because DOCS_AND_FREQS and DOCS_AND_CUSTOM_FREQS, which are encoded using
   * the same bits, are treated as distinct options due to their different interpretations.
   */
  public boolean subsumes(IndexOptions other) {
    // treat DOCS_AND_CUSTOM_FREQS and DOCS_AND_FREQS for the purpose of subsumes
    if (this == DOCS_AND_CUSTOM_FREQS) {
      return DOCS_AND_FREQS.subsumes(other);
    }
    if (other == DOCS_AND_CUSTOM_FREQS) {
      return subsumes(DOCS_AND_FREQS);
    }
    return compareTo(other) >= 0;
  }
}
