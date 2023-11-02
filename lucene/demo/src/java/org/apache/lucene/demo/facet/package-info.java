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
 * Facet Userguide and Demo.
 *
 * <h2>Table of Contents</h2>
 *
 * <ol>
 *   <li><a href="#intro">Introduction</a>
 *   <li><a href="#facet_features">Facet Features</a>
 *       <ol>
 *         <li><a href="#facet_sounting">Facet Counting</a>
 *         <li><a href="#facet_association">Facet Associations</a>
 *       </ol>
 *   <li><a href="#facet_indexing">Indexing Categories Illustrated</a>
 *   <li><a href="#facet_collection">Collecting Facets Illustrated</a>
 *   <li><a href="#indexed_facet_info">Indexed Facet Information</a>
 *       <ol>
 *         <li><a href="#category_terms_field">Category Terms Field</a>
 *         <li><a href="#category_list_field">Category List Field</a>
 *       </ol>
 *   <li><a href="#taxonomy_index">Taxonomy Index</a>
 *   <li><a href="#facet_config">Facet Configuration</a>
 *   <li><a href="#advanced">Advanced Faceted Examples</a>
 *       <ol>
 *         <li><a href="#drill_down">Drill-Down with Regular Facets</a>
 *         <li><a href="#multi-category_list">Multiple Category Lists</a>
 *         <li><a href="#sampling">Sampling</a>
 *       </ol>
 *   <li><a href="#concurrent_indexing_search">Concurrent Indexing and Search</a>
 *   <li><a href="#demo">All demo packages and classes</a>
 * </ol>
 *
 * <h2 id="intro">Introduction</h2>
 *
 * <p>A category is an aspect of indexed documents which can be used to classify the documents. For
 * example, in a collection of books at an online bookstore, categories of a book can be its price,
 * author, publication date, binding type, and so on.
 *
 * <p>In faceted search, in addition to the standard set of search results, we also get facet
 * results, which are lists of subcategories for certain categories. For example, for the price
 * facet, we get a list of relevant price ranges; for the author facet, we get a list of relevant
 * authors; and so on. In most UIs, when users click one of these subcategories, the search is
 * narrowed, or drilled down, and a new search limited to this subcategory (e.g., to a specific
 * price range or author) is performed.
 *
 * <p>Note that faceted search is more than just the ordinary fielded search. In fielded search,
 * users can add search keywords like price:10 or author:"Mark Twain" to the query to narrow the
 * search, but this requires knowledge of which fields are available, and which values are worth
 * trying. This is where faceted search comes in: it provides a list of useful subcategories, which
 * ensures that the user only drills down into useful subcategories and never into a category for
 * which there are no results. In essence, faceted search makes it easy to navigate through the
 * search results. The list of subcategories provided for each facet is also useful to the user in
 * itself, even when the user never drills down. This list allows the user to see at one glance some
 * statistics on the search results, e.g., what price ranges and which authors are most relevant to
 * the given query.
 *
 * <p>In recent years, faceted search has become a very common UI feature in search engines,
 * especially in e-commerce websites. Faceted search makes it easy for untrained users to find the
 * specific item they are interested in, whereas manually adding search keywords (as in the examples
 * above) proved too cumbersome for ordinary users, and required too much guesswork,
 * trial-and-error, or the reading of lengthy help pages.
 *
 * <p>See <a
 * href="http://en.wikipedia.org/wiki/Faceted_search">http://en.wikipedia.org/wiki/Faceted_search</a>
 * for more information on faceted search.
 *
 * <h2 id="facet_features">Facet Features</h2>
 *
 * First and main faceted search capability that comes to mind is counting, but in fact faceted
 * search is more than facet counting. We now briefly discuss the available faceted search features.
 *
 * <h3 id="facet_sounting">Facet Counting</h3>
 *
 * <p>Which of the available subcategories of a facet should a UI display? A query in a book store
 * might yield books by a hundred different authors, but normally we'd want do display only, say,
 * ten of those.
 *
 * <p>Most available faceted search implementations use counts to determine the importance of each
 * subcategory. These implementations go over all search results for the given query, and count how
 * many results are in each subcategory. Finally, the subcategories with the most results can be
 * displayed. So the user sees the price ranges, authors, and so on, for which there are most
 * results. Often, the count is displayed next to the subcategory name, in parentheses, telling the
 * user how many results he can expect to see if he drills down into this subcategory.
 *
 * <p>The main API for obtaining facet counting is {@link org.apache.lucene.facet.Facets}.
 *
 * <p>Code examples can be found in <a
 * href="../../../../../src-html/org/apache/lucene/demo/facet/SimpleFacetsExample.html">SimpleFacetsExample.java</a>,
 * see details in <a href="#facet_collection">Collecting Facets</a> section.
 *
 * <h3 id="facet_association">Facet Associations</h3>
 *
 * <p>So far we've discussed categories as binary features, where a document either belongs to a
 * category, or not.
 *
 * <p>While counts are useful in most situations, they are sometimes not sufficiently informative
 * for the user, with respect to deciding which subcategory is more important to display.
 *
 * <p>For this, the facets package allows to associate a value with a category. The search time
 * interpretation of the associated value is application dependent. For example, a possible
 * interpretation is as a <i>match level</i> (e.g., confidence level). This value can then be used
 * so that a document that is very weakly associated with a certain category will only contribute
 * little to this category's aggregated weight.
 *
 * <h2 id="facet_indexing">Indexing Categories Illustrated</h2>
 *
 * <p>In order to find facets at search time they must first be added to the index at indexing time.
 * Recall that Lucene documents are made of fields. To index document categories you use special
 * field, {@link org.apache.lucene.facet.FacetField}. The field requires following parameters:
 *
 * <ul>
 *   <li>Facet <b>dimension</b>, for example <i>author</i> or <i>publication date</i>.
 *   <li>Facet <b>path</b> from root to a leaf for the current document. For example, for
 *       <i>publication date</i> dimension the path can be <i>&lt;"2010", "07", "28"&gt;</i>.
 *       Constructed this way, this path allows us to refine search or counting for all books,
 *       published in the same year, or in the same year and month.
 * </ul>
 *
 * From taxonomy point of view, <i>dimension</i> is just a root element - or the top, the first
 * element - in a category path. Indeed, the dimension stands out as a top important part of the
 * category path, such as <code>"Location"</code> for the category <code>
 * "Location/Europe/France/Paris"</code>.
 *
 * <p>After all facet fields are added to the document, you should translate them into "normal"
 * fields for indexing and, if required, updates taxonomy index. To do that, you need to call {@link
 * org.apache.lucene.facet.FacetsConfig#build(org.apache.lucene.facet.taxonomy.TaxonomyWriter,
 * org.apache.lucene.document.Document) FacetsConfig.build(...)}. Before building, you might want to
 * customize the per-dimension facets configuration, see details in <a
 * href="#indexed_facet_info">Indexed Facet Information</a>.
 *
 * <p>Indexing of each document therefore usually goes like this:
 *
 * <ul>
 *   <li>Create a fresh (empty) Lucene Document.
 *   <li>Parse input attributes and add appropriate index fields.
 *   <li>Add all input categories associated with the document as {@link
 *       org.apache.lucene.facet.FacetField} fields to the Lucene document.
 *   <li>Build {@linkplain org.apache.lucene.facet.FacetField facet fields} with {@link
 *       org.apache.lucene.facet.FacetsConfig#build(org.apache.lucene.facet.taxonomy.TaxonomyWriter,
 *       org.apache.lucene.document.Document) FacetsConfig.build(...)}. This actually adds the
 *       categories to the Lucene document and, if required, updates taxonomy to contain the newly
 *       added categories (if not already there) - see more on this in the section about the <a
 *       href="#taxonomy_index">Taxonomy Index</a> below.
 *   <li>Add the document to the index. As a result, category info is saved also in the regular
 *       search index, for supporting facet aggregation at search time (e.g. facet counting) as well
 *       as facet drill-down. For more information on indexed facet information see below the
 *       section <a href="#indexed_facet_info">Indexed Facet Information</a>.
 * </ul>
 *
 * There is a category indexing code example in {@link
 * org.apache.lucene.demo.facet.SimpleFacetsExample#index()}, see <a
 * href="../../../../../src-html/org/apache/lucene/demo/facet/SimpleFacetsExample.html">SimpleFacetsExample.java
 * source code</a>.
 *
 * <h2 id="facet_collection">Collecting Facets Illustrated</h2>
 *
 * <p>Facets collection reflects a set of documents over some facet requests:
 *
 * <ul>
 *   <li>Document set - a subset of the index documents, usually documents matching a user query.
 *   <li>Facet requests - facet collection specification, e.g. count a certain facet
 *       <i>dimension</i>.
 * </ul>
 *
 * <p>{@link org.apache.lucene.facet.Facets} is a basic component in faceted search. It provides
 * multiple methods to get facet results, most of these methods take <i>dimension</i> and
 * <i>path</i> which correspond to dimensions and paths of indexed documents, see <a
 * href="#facet_indexing">Indexing Categories</a>. For example, {@link
 * org.apache.lucene.facet.Facets#getTopChildren(int, java.lang.String, java.lang.String...)
 * getTopChildren(10, "Publish Date", "2010", "07")} can be used to return top 10 labels for books
 * published in July 2010.
 *
 * <p>{@link org.apache.lucene.facet.Facets} in an abstract class, open for extensions. The most
 * often used implementation is {@link org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts}, it
 * is used for counting facets.
 *
 * <p><b>NOTE</b> You must make sure that {@link org.apache.lucene.facet.FacetsConfig} used for
 * searching matches the one that was used for indexing.
 *
 * <p>Facet collectors collect hits for subsequent faceting. The most commonly used one is {@link
 * org.apache.lucene.facet.FacetsCollector}. The collectors extend {@link
 * org.apache.lucene.search.Collector}, and as such can be passed to the search() method of Lucene's
 * {@link org.apache.lucene.search.IndexSearcher}. In case the application also needs to collect
 * documents (in addition to accumulating/collecting facets), you can use one of {@link
 * org.apache.lucene.facet.FacetsCollector#search(org.apache.lucene.search.IndexSearcher,
 * org.apache.lucene.search.Query, int, org.apache.lucene.search.Collector)
 * FacetsCollector.search(...)} utility methods.
 *
 * <p>There is a facets collecting code example in {@link
 * org.apache.lucene.demo.facet.SimpleFacetsExample#facetsWithSearch()}, see <a
 * href="../../../../../src-html/org/apache/lucene/demo/facet/SimpleFacetsExample.html">SimpleFacetsExample.java
 * source code</a>.
 *
 * <p>Returned {@link org.apache.lucene.facet.FacetResult} instances contain:
 *
 * <ul>
 *   <li>Requested {@linkplain org.apache.lucene.facet.FacetResult#dim dimension} and {@linkplain
 *       org.apache.lucene.facet.FacetResult#path path}.
 *   <li>Total {@linkplain org.apache.lucene.facet.FacetResult#value number of documents} containing
 *       a value for this path and {@linkplain org.apache.lucene.facet.FacetResult#childCount number
 *       of child labels encountered}.
 *   <li>And last but not least, {@linkplain org.apache.lucene.facet.FacetResult#labelValues
 *       children labels and counts}.
 * </ul>
 *
 * <h2 id="indexed_facet_info">Indexed Facet Information</h2>
 *
 * <p>When indexing a document with facet fields (categories), information on these categories is
 * added to the search index, in two locations:
 *
 * <h3 id="category_terms_field">Category Terms Field</h3>
 *
 * Category terms, or drill-down terms, are added to the document that contains facets fields. These
 * categories can be used at search time for drill-down.
 *
 * <p>{@link org.apache.lucene.facet.FacetsConfig} has a per-dimension config of the Category Terms
 * field, e.g. you can choose to {@linkplain
 * org.apache.lucene.facet.FacetsConfig.DrillDownTermsIndexing#NONE not index them at all} or
 * {@linkplain org.apache.lucene.facet.FacetsConfig.DrillDownTermsIndexing#ALL index dimension, all
 * sub-paths and full path}, default config). For example, indexing a document with a dimension
 * <code>"author"</code> and path <code>&lt;"American", "Mark Twain"&gt;</code> results in creating
 * three tokens: <code>"/author"</code>, <code>"/author/American"</code>, and <code>
 * "/author/American/Mark Twain"</code> (the character <code>'/'</code> here is just a
 * human-readable separator, there's no such element in the actual index). This allows drilling down
 * any category in the taxonomy, and not just leaf nodes.
 *
 * <h3 id="category_list_field">Category List Field</h3>
 *
 * Category List field is added to each document containing information on the categories that were
 * added to this document. This can be used at search time for facet accumulation, e.g. facet
 * counting.
 *
 * <p>If dimension is hierarchical (see {@link
 * org.apache.lucene.facet.FacetsConfig.DimConfig#hierarchical}), the field allows counting any
 * sub-category in the taxonomy, and not just leaf nodes, e.g. in the example above it enables a UI
 * application to show either how many books have authors, or how many books have American authors,
 * or how many books have Mark Twain as their (American) author.
 *
 * <p>If separate taxonomy index is used (see <a href="#taxonomy_index">Taxonomy Index</a> for when
 * it's not), in order to keep the counting list compact, Category List field is built using
 * category ordinal - an ordinal is an integer number attached to a category when it is added for
 * the first time into the taxonomy.
 *
 * <p>For ways to further alter facet index see the section below on <a
 * href="#indexing_params">Facet Indexing Parameters</a>.
 *
 * <h2 id="taxonomy_index">Taxonomy Index</h2>
 *
 * <p>The taxonomy is an auxiliary data-structure that can be maintained side-by-side with the
 * regular index to support faceted search operations. It contains information about all the
 * categories that ever existed in any document in the index. Its API is open and allows simple
 * usage, or more advanced for the interested users.
 *
 * <p>Not all Facet field types use Taxonomy Index to store data. {@link
 * org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField} writes taxonomy data as a {@link
 * org.apache.lucene.index.SortedSetDocValues} field in the regular index, therefore if you only use
 * these fields for taxonomy, you don't need taxonomy index and its writer. All details in this
 * section below are only applicable to cases where Taxonomy Index is created.
 *
 * <p>When {@link
 * org.apache.lucene.facet.FacetsConfig#build(org.apache.lucene.facet.taxonomy.TaxonomyWriter,
 * org.apache.lucene.document.Document) FacetsConfig.build(...)} is called on a document with
 * {@linkplain org.apache.lucene.facet.FacetField facet fields}, a corresponding node is added to
 * the taxonomy index (unless already there). In fact, sometimes more than one node is added - each
 * parent category is added as well, so that the taxonomy is maintained as a Tree, with a virtual
 * root.
 *
 * <p>So, for the above example, adding the category <code>
 * &lt;"author", "American", "Mark Twain"&gt;</code> actually added three nodes: one for the
 * dimension <code>"/author"</code>, one for <code>"/author/American"</code> and one for <code>
 * "/author/American/Mark Twain"</code>.
 *
 * <p>An integer number - called ordinal is attached to each category the first time the category is
 * added to the taxonomy. This allows for a compact representation of category list tokens in the
 * index, for facets accumulation.
 *
 * <p>One interesting fact about the taxonomy index is worth knowing: once a category is added to
 * the taxonomy, it is never removed, even if all related documents are removed. This differs from a
 * regular index, where if all documents containing a certain term are removed, and their segments
 * are merged, the term will also be removed. This might cause a performance issue: large taxonomy
 * means large ordinal numbers for categories, and hence large categories values arrays would be
 * maintained during accumulation. It is probably not a real problem for most applications, but be
 * aware of this. If, for example, an application at a certain point in time removes an index
 * entirely in order to recreate it, or, if it removed all the documents from the index in order to
 * re-populate it, it also makes sense in this opportunity to remove the taxonomy index and create a
 * new, fresh one, without the unused categories.
 *
 * <h2 id="facet_config">Facet Configuration</h2>
 *
 * <p>Facet configuration controls how categories and facets are indexed and searched. It is not
 * required to provide any parameters, as there are ready to use working defaults for everything.
 * However, some aspects are configurable and can be modified by providing altered facet
 * configuration parameters for search and indexing.
 *
 * <p>The most often used configuration options are:
 *
 * <ul>
 *   <li>{@linkplain
 *       org.apache.lucene.facet.FacetsConfig#setDrillDownTermsIndexing(java.lang.String,
 *       org.apache.lucene.facet.FacetsConfig.DrillDownTermsIndexing) Drill-Down Terms Indexing}
 *       level - defines what is written to <a href="#category_terms_field">Category Terms
 *       Field</a>.
 *   <li>{@linkplain org.apache.lucene.facet.FacetsConfig#setHierarchical(java.lang.String, boolean)
 *       Hierarchical} - Mark selected dimension as hierarchical, to support counting
 *       sub-categories.
 *   <li>{@linkplain org.apache.lucene.facet.FacetsConfig#setMultiValued(java.lang.String, boolean)
 *       MultiValued} - Allow multiple values per document per dimension. Multi value support
 *       requires storing all parent category ordinals in the <a
 *       href="#category_list_field">category list field</a> for accurate counting, so it has to be
 *       configured explicitly.
 * </ul>
 *
 * <h2 id="advanced">Advanced Faceted Examples</h2>
 *
 * <p>We now provide examples for more advanced facet indexing and search, such as drilling-down on
 * facet values and multiple category lists.
 *
 * <h3 id="drill_down">Drill-Down with Regular Facets</h3>
 *
 * <p>Drill-down allows users to focus on part of the results. Assume a commercial sport equipment
 * site where a user is searching for a tennis racquet. The user issues the query <i>tennis
 * racquet</i> and as result is shown a page with 10 tennis racquets, by various providers, of
 * various types and prices. In addition, the site UI shows to the user a break-down of all
 * available racquets by price and make. The user now decides to focus on racquets made by
 * <i>Head</i>, and will now be shown a new page, with 10 Head racquets, and new break down of the
 * results into racquet types and prices. Additionally, the application can choose to display a new
 * breakdown, by racquet weights. This step of moving from results (and facet statistics) of the
 * entire (or larger) data set into a portion of it by specifying a certain category, is what we
 * call <i>Drilldown</i>.
 *
 * <p>You can find code example for drill-down in {@link
 * org.apache.lucene.demo.facet.SimpleFacetsExample#drillDown()}, see <a
 * href="../../../../../src-html/org/apache/lucene/demo/facet/SimpleFacetsExample.html">SimpleFacetsExample.java
 * source code</a>.
 *
 * <h3 id="multi-category_list">Multiple Category Lists</h3>
 *
 * The default is to maintain all <a href="#category_list_field">category list</a> information in a
 * single field. While this will suit most applications, in some situations an application may wish
 * to use multiple fields, for example, when the distribution of some category values is different
 * from that of other categories and calls for using a different encoding, more efficient for the
 * specific distribution. Another example is when most facets are rarely used while some facets are
 * used very heavily, so an application may opt to maintain the latter in memory - and in order to
 * keep memory footprint lower it is useful to maintain only those heavily used facets in a separate
 * category list.
 *
 * <p>You can find full example code in <a
 * href="../../../../../src-html/org/apache/lucene/demo/facet/MultiCategoryListsFacetsExample.html">MultiCategoryListsFacetsExample.java</a>.
 *
 * <p>First we need to change facets configuration to use different fields for different dimensions.
 *
 * <p>This will cause the Author categories to be maintained in one category list field, and Publish
 * Date facets to be maintained in a another field. Note that any other category, if encountered,
 * will still be maintained in the default field.
 *
 * <p>These non-default facets parameters should now be used both at indexing and search time, so
 * make sure you use the same or similar {@link org.apache.lucene.facet.FacetsConfig} in both cases.
 *
 * <h3 id="sampling">Sampling</h3>
 *
 * Faceted search through a large collection of documents with large numbers of facets altogether
 * and/or large numbers of facets per document is challenging performance wise, either in CPU, RAM,
 * or both.
 *
 * <p>Facet sampling allows to accumulate facets over a sample of the matching documents set. In
 * many cases, once top facets are found over the sample set, exact accumulations are computed for
 * those facets only, this time over the entire matching document set.
 *
 * <p>Sampling support is implemented in {@link
 * org.apache.lucene.facet.RandomSamplingFacetsCollector}.
 *
 * <h2 id="concurrent_indexing_search">Concurrent Indexing and Search</h2>
 *
 * <p>Sometimes, indexing is done once, and when the index is fully prepared, searching starts.
 * However, in most real applications indexing is <i>incremental</i> (new data comes in once in a
 * while, and needs to be indexed), and indexing often needs to happen while searching is continuing
 * at full steam.
 *
 * <p>Luckily, Lucene supports multiprocessing - one process writing to an index while another is
 * reading from it. One of the key insights behind how Lucene allows multiprocessing is <i>Point In
 * Time</i> semantics. The idea is that when an {@link org.apache.lucene.index.IndexReader} is
 * opened, it gets a view of the index at the <i>point in time</i> it was opened. If an {@link
 * org.apache.lucene.index.IndexWriter} in a different process or thread modifies the index, the
 * reader does not know about it until a new {@link org.apache.lucene.index.IndexReader} is opened.
 *
 * <p>In faceted search, we complicate things somewhat by adding a second index - the taxonomy
 * index. The taxonomy API also follows point-in-time semantics, but this is not quite enough. Some
 * attention must be paid by the user to keep those two indexes consistently in sync.
 *
 * <p>The main index refers to category numbers defined in the taxonomy index. Therefore, it is
 * important that we open the {@link org.apache.lucene.facet.taxonomy.TaxonomyReader} <i>after</i>
 * opening the {@link org.apache.lucene.index.IndexReader}. Moreover, every time an IndexReader is
 * reopened, the TaxonomyReader needs to be {@linkplain
 * org.apache.lucene.facet.taxonomy.TaxonomyReader#openIfChanged reopened} as well.
 *
 * <p>But there is one extra caution: whenever the application deems it has written enough
 * information worthy a commit, it must <b>first</b> call {@link
 * org.apache.lucene.facet.taxonomy.TaxonomyWriter#commit()} and only <b>after</b> that call {@link
 * org.apache.lucene.index.IndexWriter#commit()}. Closing the indices should also be done in this
 * order - <b>first</b> close the taxonomy, and only <b>after</b> that close the index.
 *
 * <p>Note that the above discussion assumes that the underlying file-system on which the index and
 * the taxonomy are stored respects ordering: if index A is written before index B, then any reader
 * finding a modified index B will also see a modified index A.
 *
 * <h2 id="demo">All demo packages and classes</h2>
 */
package org.apache.lucene.demo.facet;
