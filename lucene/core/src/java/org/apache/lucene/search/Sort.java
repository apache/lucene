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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Encapsulates sort criteria for returned hits.
 *
 * <p>A {@link Sort} can be created with an empty constructor, yielding an object that will instruct
 * searches to return their hits sorted by relevance; or it can be created with one or more {@link
 * SortField}s.
 *
 * @see SortField
 * @since lucene 1.4
 */
public final class Sort {

  /**
   * Represents sorting by computed relevance. Using this sort criteria returns the same results as
   * calling {@link IndexSearcher#search(Query,int) IndexSearcher#search()}without a sort criteria,
   * only with slightly more overhead.
   */
  public static final Sort RELEVANCE = new Sort();

  /** Represents sorting by index order. */
  public static final Sort INDEXORDER = new Sort(SortField.FIELD_DOC);

  // internal representation of the sort criteria
  private final SortField[] fields;
  private final String parentField;

  /**
   * Sorts by computed relevance. This is the same sort criteria as calling {@link
   * IndexSearcher#search(Query,int) IndexSearcher#search()}without a sort criteria, only with
   * slightly more overhead.
   */
  public Sort() {
    this(SortField.FIELD_SCORE);
  }

  /**
   * Sets the sort to the given criteria in succession: the first SortField is checked first, but if
   * it produces a tie, then the second SortField is used to break the tie, etc. Finally, if there
   * is still a tie after all SortFields are checked, the internal Lucene docid is used to break it.
   */
  public Sort(SortField... fields) {
    this(null, fields);
  }

  /**
   * Sets the sort to the given criteria in succession: the first SortField is checked first, but if
   * it produces a tie, then the second SortField is used to break the tie, etc. Finally, if there
   * is still a tie after all SortFields are checked, the internal Lucene docid is used to break it.
   *
   * @param parentField the name of a numeric doc values field that marks the last document of a
   *     document blocks indexed with {@link
   *     org.apache.lucene.index.IndexWriter#addDocuments(Iterable)} or it's update relatives at
   *     index time. This is required for indices that use index sorting in combination with
   *     document blocks in order to maintain the document order of the blocks documents. Index
   *     sorting will effectively compare the parent (last document) of a block in order to stable
   *     sort all it's adjacent documents that belong to a block. This field must be a numeric doc
   *     values field.
   */
  public Sort(String parentField, SortField... fields) {
    if (fields.length == 0) {
      throw new IllegalArgumentException("There must be at least 1 sort field");
    }
    this.fields = fields;
    this.parentField = parentField;
  }

  /**
   * Representation of the sort criteria.
   *
   * @return Array of SortField objects used in this sort criteria
   */
  public SortField[] getSort() {
    return fields;
  }

  /** Returns the field name that marks a document as a parent in a document block. */
  public String getParentField() {
    return parentField;
  }

  /**
   * Rewrites the SortFields in this Sort, returning a new Sort if any of the fields changes during
   * their rewriting.
   *
   * @param searcher IndexSearcher to use in the rewriting
   * @return {@code this} if the Sort/Fields have not changed, or a new Sort if there is a change
   * @throws IOException Can be thrown by the rewriting
   * @lucene.experimental
   */
  public Sort rewrite(IndexSearcher searcher) throws IOException {
    boolean changed = false;
    if (parentField != null) {
      throw new IllegalStateException("parentFields must not be used with search time sorting");
    }
    SortField[] rewrittenSortFields = new SortField[fields.length];
    for (int i = 0; i < fields.length; i++) {
      rewrittenSortFields[i] = fields[i].rewrite(searcher);
      if (fields[i] != rewrittenSortFields[i]) {
        changed = true;
      }
    }

    return changed ? new Sort(rewrittenSortFields) : this;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    if (parentField != null) {
      buffer.append("parent field: ").append(parentField).append(' ');
    }
    for (int i = 0; i < fields.length; i++) {
      buffer.append(fields[i].toString());
      if ((i + 1) < fields.length) buffer.append(',');
    }

    return buffer.toString();
  }

  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Sort)) return false;
    final Sort other = (Sort) o;
    return Objects.equals(parentField, other.parentField)
        && Arrays.equals(this.fields, other.fields);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return 0x45aaf665 + Arrays.hashCode(fields) + Objects.hashCode(parentField);
  }

  /** Returns true if the relevance score is needed to sort documents. */
  public boolean needsScores() {
    for (SortField sortField : fields) {
      if (sortField.needsScores()) {
        return true;
      }
    }
    return false;
  }
}
