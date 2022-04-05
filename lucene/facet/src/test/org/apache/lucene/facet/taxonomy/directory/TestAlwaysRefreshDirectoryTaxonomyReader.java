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
import java.nio.file.Path;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

public class TestAlwaysRefreshDirectoryTaxonomyReader extends FacetTestCase {

  /**
   * Tests the behavior of the {@link AlwaysRefreshDirectoryTaxonomyReader} by testing if the
   * associated {@link SearcherTaxonomyManager} can successfully refresh and serve queries if the
   * underlying taxonomy index is changed to an older checkpoint. Ideally, each checkpoint should be
   * self-sufficient and should allow serving search queries when {@link
   * SearcherTaxonomyManager#maybeRefresh()} is called.
   *
   * <p>It does not check whether the private taxoArrays were actually recreated or no. We are
   * (correctly) hiding away that complexity away from the user.
   */
  public void testAlwaysRefreshDirectoryTaxonomyReader() throws IOException {
    final Path taxoPath1 = createTempDir("dir1");
    final Directory dir1 = newFSDirectory(taxoPath1);
    final DirectoryTaxonomyWriter tw1 =
        new DirectoryTaxonomyWriter(dir1, IndexWriterConfig.OpenMode.CREATE);
    tw1.addCategory(new FacetLabel("a"));
    tw1.commit(); // commit1

    final Path taxoPath2 = createTempDir("commit1");
    final Directory commit1 = newFSDirectory(taxoPath2);
    // copy all index files from dir1
    for (String file : dir1.listAll()) {
      commit1.copyFrom(dir1, file, file, IOContext.READ);
    }

    tw1.addCategory(new FacetLabel("b"));
    tw1.commit(); // commit2
    tw1.close();

    final DirectoryReader dr1 = DirectoryReader.open(dir1);
    // using a DirectoryTaxonomyReader here will cause the test to fail and throw a AIOOB exception
    // in maybeRefresh()
    final DirectoryTaxonomyReader dtr1 = new AlwaysRefreshDirectoryTaxonomyReader(dir1);
    final SearcherTaxonomyManager mgr = new SearcherTaxonomyManager(dr1, dtr1, null);

    final FacetsConfig config = new FacetsConfig();
    final SearcherTaxonomyManager.SearcherAndTaxonomy pair = mgr.acquire();
    final FacetsCollector sfc = new FacetsCollector();
    /**
     * the call flow here initializes {@link DirectoryTaxonomyReader#taxoArrays}. These reused
     * `taxoArrays` form the basis of the inconsistency *
     */
    getTaxonomyFacetCounts(pair.taxonomyReader, config, sfc);

    // now try to go back to checkpoint 1 and refresh the SearcherTaxonomyManager

    // delete all files from commit2
    for (String file : dir1.listAll()) {
      dir1.deleteFile(file);
    }

    // copy all index files from commit1
    for (String file : commit1.listAll()) {
      dir1.copyFrom(commit1, file, file, IOContext.READ);
    }

    mgr.maybeRefresh();
    IOUtils.close(mgr, dtr1, dr1);
    // closing commit1 and dir1 throws exceptions because of checksum mismatches
    IOUtils.closeWhileHandlingException(commit1, dir1);
  }
}
