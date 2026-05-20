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
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.RefreshCommitSupplier;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Manages near-real-time reopen of both an IndexSearcher and a TaxonomyReader.
 *
 * <p><b>NOTE</b>: If you call {@link DirectoryTaxonomyWriter#replaceTaxonomy} then you must open a
 * new {@code SearcherTaxonomyManager} afterwards.
 */
public class SearcherTaxonomyManager
    extends ReferenceManager<SearcherTaxonomyManager.SearcherAndTaxonomy> {

  /**
   * Holds a matched pair of {@link IndexSearcher} and {@link TaxonomyReader}
   *
   * @param searcher Point-in-time {@link IndexSearcher}.
   * @param taxonomyReader Matching point-in-time {@link DirectoryTaxonomyReader}.
   */
  public record SearcherAndTaxonomy(
      IndexSearcher searcher, DirectoryTaxonomyReader taxonomyReader) {}

  private final SearcherFactory searcherFactory;
  private final long taxoEpoch;
  private final DirectoryTaxonomyWriter taxoWriter;
  private RefreshCommitSupplier refreshCommitSupplier = new RefreshCommitSupplier() {};

  /** Creates near-real-time searcher and taxonomy reader from the corresponding writers. */
  public SearcherTaxonomyManager(
      IndexWriter writer, SearcherFactory searcherFactory, DirectoryTaxonomyWriter taxoWriter)
      throws IOException {
    this(writer, true, searcherFactory, taxoWriter);
  }

  /**
   * Expert: creates near-real-time searcher and taxonomy reader from the corresponding writers,
   * controlling whether deletes should be applied.
   */
  public SearcherTaxonomyManager(
      IndexWriter writer,
      boolean applyAllDeletes,
      SearcherFactory searcherFactory,
      DirectoryTaxonomyWriter taxoWriter)
      throws IOException {
    this(writer, applyAllDeletes, searcherFactory, taxoWriter, null);
  }

  /**
   * Expert: creates near-real-time searcher and taxonomy reader from the corresponding writers,
   * controlling whether deletes should be applied.
   */
  public SearcherTaxonomyManager(
      IndexWriter writer,
      boolean applyAllDeletes,
      SearcherFactory searcherFactory,
      DirectoryTaxonomyWriter taxoWriter,
      RefreshCommitSupplier refreshCommitSupplier)
      throws IOException {
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    this.taxoWriter = taxoWriter;
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
    current =
        new SearcherAndTaxonomy(
            SearcherManager.getSearcher(
                searcherFactory, DirectoryReader.open(writer, applyAllDeletes, false), null),
            taxoReader);
    this.taxoEpoch = taxoWriter.getTaxonomyEpoch();
    if (refreshCommitSupplier != null) {
      this.refreshCommitSupplier = refreshCommitSupplier;
    }
  }

  /**
   * Creates search and taxonomy readers over the corresponding directories.
   *
   * <p><b>NOTE:</b> you should only use this constructor if you commit and call {@link
   * #maybeRefresh()} in the same thread. Otherwise it could lead to an unsync'd {@link
   * IndexSearcher} and {@link TaxonomyReader} pair.
   */
  public SearcherTaxonomyManager(
      Directory indexDir, Directory taxoDir, SearcherFactory searcherFactory) throws IOException {
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    current =
        new SearcherAndTaxonomy(
            SearcherManager.getSearcher(searcherFactory, DirectoryReader.open(indexDir), null),
            taxoReader);
    this.taxoWriter = null;
    taxoEpoch = -1;
  }

  /**
   * Creates this from already opened {@link IndexReader} and {@link DirectoryTaxonomyReader}
   * instances. Note that the incoming readers will be closed when you call {@link #close}.
   */
  public SearcherTaxonomyManager(
      IndexReader reader, DirectoryTaxonomyReader taxoReader, SearcherFactory searcherFactory)
      throws IOException {
    this(reader, taxoReader, searcherFactory, null);
  }

  /**
   * Creates this from already opened {@link IndexReader} and {@link DirectoryTaxonomyReader}
   * instances. Note that the incoming readers will be closed when you call {@link #close}.
   */
  public SearcherTaxonomyManager(
      IndexReader reader,
      DirectoryTaxonomyReader taxoReader,
      SearcherFactory searcherFactory,
      RefreshCommitSupplier refreshCommitSupplier)
      throws IOException {
    if (searcherFactory == null) {
      searcherFactory = new SearcherFactory();
    }
    this.searcherFactory = searcherFactory;
    current =
        new SearcherAndTaxonomy(
            SearcherManager.getSearcher(searcherFactory, reader, null), taxoReader);
    this.taxoWriter = null;
    taxoEpoch = -1;
    if (refreshCommitSupplier != null) {
      this.refreshCommitSupplier = refreshCommitSupplier;
    }
  }

  @Override
  protected void decRef(SearcherAndTaxonomy ref) throws IOException {
    ref.searcher.getIndexReader().decRef();

    // This decRef can fail, and then in theory we should
    // tryIncRef the searcher to put back the ref count
    // ... but 1) the below decRef should only fail because
    // it decRef'd to 0 and closed and hit some IOException
    // during close, in which case 2) very likely the
    // searcher was also just closed by the above decRef and
    // a tryIncRef would fail:
    ref.taxonomyReader.decRef();
  }

  @Override
  protected boolean tryIncRef(SearcherAndTaxonomy ref) throws IOException {
    if (ref.searcher.getIndexReader().tryIncRef()) {
      if (ref.taxonomyReader.tryIncRef()) {
        return true;
      } else {
        ref.searcher.getIndexReader().decRef();
      }
    }
    return false;
  }

  @Override
  protected SearcherAndTaxonomy refreshIfNeeded(SearcherAndTaxonomy ref) throws IOException {
    // Must re-open searcher first, otherwise we may get a
    // new reader that references ords not yet known to the
    // taxonomy reader:
    final IndexReader r = ref.searcher.getIndexReader();
    DirectoryReader dr = (DirectoryReader) r;
    IndexCommit refreshCommit = refreshCommitSupplier.getSearcherRefreshCommit(dr);
    IndexReader newReader = DirectoryReader.openIfChanged(dr, refreshCommit);
    if (newReader == null) {
      return null;
    } else {
      DirectoryTaxonomyReader tr = null;
      // taxonomy should always be ahead of searchers. Otherwise, searchers
      // might reference ordinals that taxonomy doesn't know of.
      // To ensure this, we always refresh taxonomy on the latest commit.
      try {
        tr = TaxonomyReader.openIfChanged(ref.taxonomyReader);
      } catch (Throwable t1) {
        try {
          IOUtils.close(newReader);
        } catch (Throwable t2) {
          t2.addSuppressed(t2);
        }
        throw t1;
      }
      if (tr == null) {
        ref.taxonomyReader.incRef();
        tr = ref.taxonomyReader;
      } else if (taxoWriter != null && taxoWriter.getTaxonomyEpoch() != taxoEpoch) {
        IOUtils.close(newReader, tr);
        throw new IllegalStateException(
            "DirectoryTaxonomyWriter.replaceTaxonomy was called, which is not allowed when using SearcherTaxonomyManager");
      }

      return new SearcherAndTaxonomy(
          SearcherManager.getSearcher(searcherFactory, newReader, r), tr);
    }
  }

  /** Return index commit generation for current searcher. pkg-private for testing */
  long getSearcherCommitGeneration() throws IOException {
    SearcherAndTaxonomy sat = acquire();
    long gen = ((DirectoryReader) sat.searcher.getIndexReader()).getIndexCommit().getGeneration();
    release(sat);
    return gen;
  }

  /**
   * Returns <code>true</code> if no new changes have occurred since the current searcher (i.e.
   * reader) was opened, <code>false</code> otherwise. pkg-private for testing
   *
   * @see DirectoryReader#isCurrent()
   */
  boolean isSearcherCurrent() throws IOException {
    final SearcherAndTaxonomy sat = acquire();
    try {
      final IndexReader r = sat.searcher.getIndexReader();
      assert r instanceof DirectoryReader
          : "searcher's IndexReader should be a DirectoryReader, but got " + r;
      return ((DirectoryReader) r).isCurrent();
    } finally {
      release(sat);
    }
  }

  /**
   * Returns <code>true</code> if no new changes have occurred since the current taxonomy reader was
   * opened, <code>false</code> otherwise. pkg-private for testing
   *
   * @see DirectoryReader#isCurrent()
   */
  boolean isTaxonomyCurrent() throws IOException {
    final SearcherAndTaxonomy sat = acquire();
    try {
      return sat.taxonomyReader.getInternalIndexReader().isCurrent();
    } finally {
      release(sat);
    }
  }

  @Override
  protected int getRefCount(SearcherAndTaxonomy reference) {
    return reference.searcher.getIndexReader().getRefCount();
  }
}
