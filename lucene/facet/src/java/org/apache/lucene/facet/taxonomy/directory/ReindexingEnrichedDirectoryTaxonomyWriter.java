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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/**
 * Use this {@link org.apache.lucene.facet.taxonomy.TaxonomyWriter} to append arbitrary fields to
 * the ordinal documents in the taxonomy. To update the custom data added to the docs, it is
 * required to {@link #reindexWithNewOrdinalData(BiConsumer)}.
 *
 * @lucene.experimental
 */
public class ReindexingEnrichedDirectoryTaxonomyWriter extends DirectoryTaxonomyWriter {
  private BiConsumer<FacetLabel, Document> ordinalDataAppender;

  /** Create a taxonomy writer that will allow editing the ordinal docs before indexing them. */
  public ReindexingEnrichedDirectoryTaxonomyWriter(
      Directory d, BiConsumer<FacetLabel, Document> ordinalDataAppender) throws IOException {
    super(d);
    this.ordinalDataAppender = ordinalDataAppender;
  }

  /** Add fields specified by the {@link #ordinalDataAppender} to the provided {@link Document}. */
  @Override
  protected void enrichOrdinalDocument(Document d, FacetLabel categoryPath) {
    if (ordinalDataAppender != null) {
      ordinalDataAppender.accept(categoryPath, d);
    }
  }

  /**
   * Make a list of all labels in the taxonomy. The index of each label in this list is the ordinal
   * which corresponds to it.
   */
  private List<FacetLabel> recordPathsInOrder(Directory d) throws IOException {
    List<FacetLabel> paths = new ArrayList<>();

    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(d);
    IndexReader taxoIndexReader = taxoReader.getInternalIndexReader();

    for (LeafReaderContext ctx : taxoIndexReader.leaves()) {
      LeafReader leafReader = ctx.reader();
      int[] ordinals = new int[leafReader.maxDoc()];
      for (int i = 0; i < ordinals.length; i++) {
        ordinals[i] = ctx.docBase + i;
      }
      FacetLabel[] labels = taxoReader.getBulkPath(ordinals);
      for (FacetLabel label : labels) {
        paths.add(label);
      }
    }

    IOUtils.close(taxoReader);
    return paths;
  }

  /**
   * Delete the existing taxonomy index and recreate it using new ordinal data. The ordinals
   * themselves will be preserved, so the caller does not need to update references to them in the
   * main index.
   */
  public synchronized void reindexWithNewOrdinalData(
      BiConsumer<FacetLabel, Document> ordinalDataAppender) throws IOException {
    ensureOpen();
    this.ordinalDataAppender = ordinalDataAppender;
    Directory d = getDirectory();

    // Record paths in order.
    List<FacetLabel> ordinalToPath = recordPathsInOrder(d);

    // Delete old taxonomy files.
    deleteAll();

    // Index paths in order - they will use the new appender.
    for (FacetLabel categoryPath : ordinalToPath) {
      addCategory(categoryPath);
    }
    commit();
  }
}
