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

package org.apache.lucene.util.hnsw;

/**
 * Implemented by collectors that
 * understand parent-child document relationships and can enumerate sibling document ids for a given
 * child document id, as well as translate document ids back to vector ordinals.
 *
 * <p>This interface is used internally by {@link OrdinalTranslatedKnnCollector} to bridge between
 * the ordinal space of the HNSW graph and the document-id space of the collector.
 *
 * @lucene.experimental
 */
//  The interface cannot be removed. It exists for a module-boundary reason.
//  DocSiblingExpansion is in lucene/core, while DiversifyingNearestChildrenKnnCollector is in lucene/join.
//  The dependency is one-way: join depends on core, never the reverse. So OrdinalTranslatedKnnCollector (in core)
//  has no way to reference DiversifyingNearestChildrenKnnCollector directly.
//
//  The interface is the bridge — it lets core call findSiblingDocIds and docIdToOrdinal on the collector without
//  creating a circular dependency. Removing it would require either moving OrdinalTranslatedKnnCollector into
//  join (bigger refactor) or adding a core → join dependency (illegal in this architecture).
public interface DocSiblingExpansion {

  /**
   * Returns the doc ids of all siblings of {@code childDocId} whose parent has not yet been added
   * to the result heap, or {@code null} if the parent was already processed or there are no other
   * siblings.
   *
   * @param childDocId the document id of the child that is about to be collected
   * @return sibling doc ids, or {@code null}
   */
  int[] findSiblingDocIds(int childDocId);

  /**
   * Translates a document id to its vector ordinal, or returns {@code -1} if the document has no
   * vector in this field.
   *
   * @param docId the document id
   * @return the vector ordinal, or {@code -1}
   */
  int docIdToOrdinal(int docId);
}
