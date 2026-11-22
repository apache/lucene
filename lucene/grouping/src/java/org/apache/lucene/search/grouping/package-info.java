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

/**
 * Grouping.
 *
 * <p>This module enables search result grouping with Lucene, where hits with the same value in the
 * specified single-valued group field are grouped together. For example, if you group by the <code>
 * author</code> field, then all documents with the same value in the <code>author</code> field fall
 * into a single group.
 *
 * <p>Grouping requires a number of inputs:
 *
 * <ul>
 *   <li><code>groupSelector</code>: this defines how groups are created from values per-document.
 *       The grouping module ships with selectors for grouping by term, and by long and double
 *       ranges.
 *   <li><code>groupSort</code>: how the groups are sorted. For sorting purposes, each group is
 *       "represented" by the highest-sorted document according to the <code>groupSort</code> within
 *       it. For example, if you specify "price" (ascending) then the first group is the one with
 *       the lowest price book within it. Or if you specify relevance group sort, then the first
 *       group is the one containing the highest scoring book.
 *   <li><code>groupOffset</code>: how many top groups to skip, in <code>groupSort</code> order.
 *       This is the group-level paging offset.
 *   <li><code>groupLimit</code>: the maximum number of groups to return, starting at the offset.
 *       The collector managers ({@link
 *       org.apache.lucene.search.grouping.FirstPassGroupingCollectorManager}, {@link
 *       org.apache.lucene.search.grouping.BlockGroupingCollectorManager}) name this parameter
 *       <code>topNGroups</code>. For example, with five groups per page, the second page uses a
 *       group offset of 5 and a group limit of 5: it skips the first five groups and returns up to
 *       the next five. The last page can be shorter when fewer groups match.
 *   <li><code>withinGroupSort</code>: how the documents within each group are sorted. This can be
 *       different from the group sort.
 *   <li><code>withinGroupOffset</code>: how many top documents to skip within each returned group
 *       ({@link org.apache.lucene.search.grouping.GroupingSearch#setGroupDocsOffset}). This pages
 *       the documents inside a group, not the groups.
 *   <li><code>maxDocsPerGroup</code>: the maximum number of documents to return per group, starting
 *       at <code>withinGroupOffset</code> ({@link
 *       org.apache.lucene.search.grouping.GroupingSearch#setGroupDocsLimit}).
 * </ul>
 *
 * <p>The implementation is two-pass: the first pass ({@link
 * org.apache.lucene.search.grouping.FirstPassGroupingCollectorManager}) gathers the top groups, and
 * the second pass ({@link org.apache.lucene.search.grouping.TopGroupsCollectorManager}) gathers
 * documents within those groups. Both passes use {@link org.apache.lucene.search.CollectorManager}
 * to support concurrent (parallel) collection across segments. If the search is costly to run you
 * may want to use the {@link org.apache.lucene.search.CachingCollectorManager} class, which wraps
 * the first-pass manager and caches all collected documents (and optionally scores) per segment
 * slice so they can be replayed into the second-pass manager without re-running the query. This way
 * you only run the query once, but you pay a RAM cost to (briefly) hold all hits. Results are
 * returned as a {@link org.apache.lucene.search.grouping.TopGroups} instance.
 *
 * <p>Groups are defined by {@link org.apache.lucene.search.grouping.GroupSelector} implementations:
 *
 * <ul>
 *   <li>{@link org.apache.lucene.search.grouping.TermGroupSelector} groups based on the value of a
 *       {@link org.apache.lucene.index.SortedDocValues} field
 *   <li>{@link org.apache.lucene.search.grouping.ValueSourceGroupSelector} groups based on the
 *       value of a {@link org.apache.lucene.queries.function.ValueSource}
 *   <li>{@link org.apache.lucene.search.grouping.DoubleRangeGroupSelector} groups based on the
 *       value of a {@link org.apache.lucene.search.DoubleValuesSource}
 *   <li>{@link org.apache.lucene.search.grouping.LongRangeGroupSelector} groups based on the value
 *       of a {@link org.apache.lucene.search.LongValuesSource}
 * </ul>
 *
 * <p>Known limitations:
 *
 * <ul>
 *   <li>Sharding is not directly supported, though is not too difficult, if you can merge the top
 *       groups and top documents per group yourself.
 * </ul>
 *
 * <p>To group by a field, index it as a single-valued {@link
 * org.apache.lucene.document.SortedDocValuesField}:
 *
 * <pre><code class="language-java">
 *   doc.add(new SortedDocValuesField("author", new BytesRef(author)));
 * </code></pre>
 *
 * <p>Typical usage for the generic two-pass grouping search looks like this using the grouping
 * convenience utility (optionally using caching for the second pass search):
 *
 * <pre><code class="language-java">
 *   GroupingSearch groupingSearch = new GroupingSearch("author");
 *   groupingSearch.setGroupSort(groupSort);
 *   // Documents to return within each group:
 *   groupingSearch.setGroupDocsOffset(0);
 *   groupingSearch.setGroupDocsLimit(docsPerGroup);
 *
 *   if (useCache) {
 *     // Sets cache in MB
 *     groupingSearch.setCachingInMB(4.0, true);
 *   }
 *
 *   if (requiredTotalGroupCount) {
 *     groupingSearch.setAllGroups(true);
 *   }
 *
 *   TermQuery query = new TermQuery(new Term("content", searchTerm));
 *   // Second page of five groups: skip the first five groups and return up to five more.
 *   int groupOffset = 5;
 *   int groupLimit = 5;
 *   TopGroups&lt;BytesRef&gt; result = groupingSearch.search(indexSearcher, query, groupOffset, groupLimit);
 *
 *   // Render groupsResult...
 *   if (requiredTotalGroupCount) {
 *     int totalGroupCount = result.totalGroupCount;
 *   }
 * </code></pre>
 *
 * <p>To use the single-pass <code>BlockGroupingCollectorManager</code>, first, at indexing time,
 * you must ensure all docs in each group are added as a block, and you have some way to find the
 * last document of each group. One simple way to do this is to add a marker binary field:
 *
 * <pre><code class="language-java">
 *   // Create Documents from your source:
 *   List&lt;Document&gt; oneGroup = ...;
 *
 *   Field groupEndField = new StringField("groupEnd", "x", Field.Store.NO);
 *   oneGroup.get(oneGroup.size()-1).add(groupEndField);
 *
 *   // You can also use writer.updateDocuments(); just be sure you
 *   // replace an entire previous doc block with this new one.  For
 *   // example, each group could have a "groupID" field, with the same
 *   // value for all docs in this group:
 *   writer.addDocuments(oneGroup);
 * </code></pre>
 *
 * Then, at search time (<code>groupOffset</code> and <code>topNGroups</code> page the groups as
 * described above; unlike <code>GroupingSearch</code>, the last argument counts documents from the
 * top of each group, including the ones skipped by <code>docOffset</code>):
 *
 * <pre><code class="language-java">
 *   Query groupEndDocs = new TermQuery(new Term("groupEnd", "x"));
 *   Weight lastDocPerGroup = indexSearcher.createWeight(
 *       indexSearcher.rewrite(groupEndDocs), ScoreMode.COMPLETE_NO_SCORES, 1);
 *   BlockGroupingCollectorManager&lt;BytesRef&gt; manager = new BlockGroupingCollectorManager&lt;&gt;(
 *       groupSort,
 *       groupOffset,
 *       topNGroups,
 *       needsScores,
 *       lastDocPerGroup,
 *       withinGroupSort,
 *       docOffset,
 *       docOffset + docsPerGroup);
 *   TopGroups groupsResult = indexSearcher.search(new TermQuery(new Term("content", searchTerm)), manager);
 *
 *   // Render groupsResult...
 * </code></pre>
 *
 * Or alternatively use the <code>GroupingSearch</code> convenience utility:
 *
 * <pre><code class="language-java">
 *   // Per search:
 *   GroupingSearch groupingSearch = new GroupingSearch(groupEndDocs);
 *   groupingSearch.setGroupSort(groupSort);
 *   groupingSearch.setIncludeMaxScore(needsScores);
 *   TermQuery query = new TermQuery(new Term("content", searchTerm));
 *   TopGroups groupsResult = groupingSearch.search(indexSearcher, query, groupOffset, groupLimit);
 *
 *   // Render groupsResult...
 * </code></pre>
 *
 * Note that the <code>groupValue</code> of each <code>GroupDocs</code> will be <code>null</code>,
 * so if you need to present this value you'll have to separately retrieve it (for example using
 * stored fields, <code>FieldCache</code>, etc.).
 *
 * <p>Another collector manager is the <code>AllGroupHeadsCollectorManager</code> that can be used
 * to retrieve all most relevant documents per group. Also known as group heads. This can be useful
 * in situations when one wants to compute group based facets / statistics on the complete query
 * result. The manager supports concurrent collection and can also be used with the <code>
 * GroupingSearch</code> convenience utility, but when one only wants to compute the most relevant
 * documents per group it is better to just use the manager directly as done here below.
 *
 * <pre><code class="language-java">
 *   AllGroupHeadsCollectorManager&lt;BytesRef&gt; manager =
 *       new AllGroupHeadsCollectorManager&lt;&gt;(
 *           () -&gt; new TermGroupSelector(groupField), sortWithinGroup);
 *   AllGroupHeadsCollectorManager.GroupHeadsResult result =
 *       indexSearcher.search(new TermQuery(new Term("content", searchTerm)), manager);
 *   // Return all group heads as int array
 *   int[] groupHeadsArray = result.retrieveGroupHeads();
 *   // Return all group heads as FixedBitSet.
 *   int maxDoc = indexSearcher.getIndexReader().maxDoc();
 *   Bits groupHeadsBitSet = result.retrieveGroupHeads(maxDoc);
 * </code></pre>
 *
 * <p>Grouped faceting is handled by {@link
 * org.apache.lucene.search.grouping.TermGroupFacetCollectorManager}. For each facet value it counts
 * how many distinct groups have at least one matching document with that value. The manager
 * supports concurrent collection; groups that span multiple search slices are counted once.
 *
 * <pre><code class="language-java">
 *   TermGroupFacetCollectorManager manager =
 *       new TermGroupFacetCollectorManager(
 *           "author", "category", false, null, offset + limit, minCount, orderByCount);
 *   GroupedFacetResult facetResult =
 *       indexSearcher.search(new TermQuery(new Term("content", searchTerm)), manager);
 *   int totalCount = facetResult.getTotalCount();
 *   int missingCount = facetResult.getTotalMissingCount();
 *   List&lt;GroupedFacetResult.FacetEntry&gt; entries = facetResult.getFacetEntries(offset, limit);
 * </code></pre>
 */
package org.apache.lucene.search.grouping;
