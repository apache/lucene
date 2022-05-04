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

import static com.carrotsearch.randomizedtesting.RandomizedTest.sleep;
import static org.apache.lucene.tests.mockfile.ExtrasFS.isExtra;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;
import org.junit.Ignore;

// Nefarious FS will delay/stop deletion of index files and this test specifically does that
@LuceneTestCase.SuppressFileSystems({"WindowsFS", "VirusCheckingFS"})
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
  private <T extends Throwable> void testAlwaysRefreshDirectoryTaxonomyReader(
      Function<Directory, DirectoryTaxonomyReader> dtrProducer, Class<T> exceptionType)
      throws IOException {
    final Path taxoPath1 = createTempDir();
    final Directory dir1 = newFSDirectory(taxoPath1);

    final DirectoryTaxonomyWriter tw1 =
        new DirectoryTaxonomyWriter(dir1, IndexWriterConfig.OpenMode.CREATE);
    tw1.addCategory(new FacetLabel("a"));
    tw1.commit(); // commit1

    final Path taxoPath2 = createTempDir();
    final Directory commit1 = newFSDirectory(taxoPath2);
    // copy all index files from dir1
    for (String file : dir1.listAll()) {
      if (isExtra(file) == false) {
        // the test framework creates these devious extra files just to chaos test the edge cases
        commit1.copyFrom(dir1, file, file, IOContext.READ);
      }
    }

    tw1.addCategory(new FacetLabel("b"));
    tw1.commit(); // commit2
    tw1.close();

    final DirectoryReader dr1 = DirectoryReader.open(dir1);
    final DirectoryTaxonomyReader dtr1 = dtrProducer.apply(dir1);
    final SearcherTaxonomyManager mgr = new SearcherTaxonomyManager(dr1, dtr1, null);

    final FacetsConfig config = new FacetsConfig();
    SearcherTaxonomyManager.SearcherAndTaxonomy pair = mgr.acquire();
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

    while (dir1.getPendingDeletions().isEmpty() == false) {
      // make the test more robust to the OS taking more time to actually delete files
      sleep(5);
    }

    // copy all index files from commit1
    for (String file : commit1.listAll()) {
      if (isExtra(file) == false) {
        dir1.copyFrom(commit1, file, file, IOContext.READ);
      }
    }

    if (exceptionType != null) {
      expectThrows(exceptionType, mgr::maybeRefresh);
    } else {
      mgr.maybeRefresh();
      pair = mgr.acquire();
      assertEquals(new FacetLabel("a"), pair.taxonomyReader.getPath(1));
      assertEquals(-1, pair.taxonomyReader.getOrdinal(new FacetLabel("b")));
    }

    mgr.release(pair);
    IOUtils.close(mgr, dtr1, dr1);
    // closing commit1 and dir1 throws exceptions because of checksum mismatches
    IOUtils.deleteFiles(commit1, List.of(commit1.listAll()));
    IOUtils.deleteFiles(dir1, List.of(dir1.listAll()));
    IOUtils.close(commit1, dir1);
  }

  @Ignore("LUCENE-10482: need to make this work on Windows too")
  public void testAlwaysRefreshDirectoryTaxonomyReader() throws IOException {
    testAlwaysRefreshDirectoryTaxonomyReader(
        (dir) -> {
          try {
            return new DirectoryTaxonomyReader(dir);
          } catch (IOException e) {
            e.printStackTrace();
          }
          return null;
        },
        ArrayIndexOutOfBoundsException.class);
    testAlwaysRefreshDirectoryTaxonomyReader(
        (dir) -> {
          try {
            return new AlwaysRefreshDirectoryTaxonomyReader(dir);
          } catch (IOException e) {
            e.printStackTrace();
          }
          return null;
        },
        null);
  }

  /**
   * A modified DirectoryTaxonomyReader that always recreates a new {@link
   * AlwaysRefreshDirectoryTaxonomyReader} instance when {@link
   * AlwaysRefreshDirectoryTaxonomyReader#doOpenIfChanged()} is called. This enables us to easily go
   * forward or backward in time by re-computing the ordinal space during each refresh. This results
   * in an always O(#facet_label) taxonomy array construction time when refresh is called.
   */
  private class AlwaysRefreshDirectoryTaxonomyReader extends DirectoryTaxonomyReader {

    AlwaysRefreshDirectoryTaxonomyReader(Directory directory) throws IOException {
      super(directory);
    }

    AlwaysRefreshDirectoryTaxonomyReader(DirectoryReader indexReader) throws IOException {
      super(indexReader, null, null, null, null);
    }

    @Override
    protected DirectoryTaxonomyReader doOpenIfChanged() throws IOException {
      boolean success = false;

      // the getInternalIndexReader() function performs the ensureOpen() check
      final DirectoryReader reader = DirectoryReader.openIfChanged(super.getInternalIndexReader());
      if (reader == null) {
        return null; // no changes in the directory at all, nothing to do
      }

      try {
        // It is important that we create an AlwaysRefreshDirectoryTaxonomyReader here and not a
        // DirectoryTaxonomyReader.
        // Returning a AlwaysRefreshDirectoryTaxonomyReader ensures that the recreated taxonomy
        // reader also uses the overridden doOpenIfChanged
        // method (that always recomputes values).
        final AlwaysRefreshDirectoryTaxonomyReader newTaxonomyReader =
            new AlwaysRefreshDirectoryTaxonomyReader(reader);
        success = true;
        return newTaxonomyReader;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(reader);
        }
      }
    }
  }
}
