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
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

public class TestAlwaysRefreshDirectoryTaxonomyReader extends FacetTestCase {

  /**
   * Tests the expert constructors in {@link DirectoryTaxonomyReader} and checks the {@link
   * DirectoryTaxonomyReader#getInternalIndexReader()} API. Also demonstrates the need for the
   * constructor and the API.
   *
   * <p>It does not check whether the private taxoArrays were actually recreated or no. We are
   * (correctly) hiding away that complexity away from the user.
   */
  public void testAlwaysRefreshDirectoryTaxonomyReader() throws IOException {
//    Directory dir = newDirectory();
//    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dir);
//    tw.addCategory(new FacetLabel("a"));
//    tw.commit();
//
//    DirectoryTaxonomyReader dtr1 = new AlwaysRefreshDirectoryTaxonomyReader(dir);
//    tw.addCategory(new FacetLabel("b"));
//    tw.commit();
//    DirectoryTaxonomyReader dtr2 = dtr1.doOpenIfChanged();
//
//    assert dtr2.getSize() == 3; // one doc is by default for the root ordinal
//    IOUtils.close(tw, dtr1, dtr2, dir);

//    Path taxoPath1 = createTempDir("dir1");
//    Directory dir1 = newFSDirectory(taxoPath1);
//    DirectoryTaxonomyWriter tw1 = new DirectoryTaxonomyWriter(dir1, IndexWriterConfig.OpenMode.CREATE);
//    tw1.addCategory(new FacetLabel("a"));
//    tw1.commit();
//    tw1.close();
//
//    DirectoryTaxonomyReader dtr1 = new AlwaysRefreshDirectoryTaxonomyReader(dir1);
//
//    Path taxoPath2 = createTempDir("dir2");
//    Directory dir2 = newFSDirectory(taxoPath2);
//    DirectoryTaxonomyWriter tw2 = new DirectoryTaxonomyWriter(dir2, IndexWriterConfig.OpenMode.CREATE);
//    tw2.addCategory(new FacetLabel("b"));
//    tw2.addCategory(new FacetLabel("c"));
//    tw2.commit();
////    tw2.addCategory(new FacetLabel("d"));
////    tw2.commit();
//    tw2.close();
//
//    // delete all files from dir1
//    for (String file : dir1.listAll()) {
//      System.out.println("initial file " + file);
//      dir1.deleteFile(file);
//    }
//
//    // copy all index files from dir2
//    for (String file : dir2.listAll()) {
//      System.out.println("copying file " + file);
//      dir1.copyFrom(dir2, file, file, IOContext.READ);
//    }
//
//    // refresh the old directory reader to see if it has gotten the updates from the new copied files
//    DirectoryTaxonomyReader dtr2 = dtr1.doOpenIfChanged();
//    assert dtr2.getSize() == 3;

    Path taxoPath1 = createTempDir("dir1");
    Directory dir1 = newFSDirectory(taxoPath1);
    DirectoryTaxonomyWriter tw1 = new DirectoryTaxonomyWriter(dir1, IndexWriterConfig.OpenMode.CREATE);
    tw1.addCategory(new FacetLabel("a"));
    tw1.addCategory(new FacetLabel("b"));
    tw1.commit();

    DirectoryReader dr1 = DirectoryReader.open(dir1);
    DirectoryTaxonomyReader dtr1 = new DirectoryTaxonomyReader(dir1);
    final SearcherTaxonomyManager mgr = new SearcherTaxonomyManager(dr1, dtr1, null);

    Path taxoPath2 = createTempDir("dir2");
    Directory dir2 = newFSDirectory(taxoPath2);
    DirectoryTaxonomyWriter tw2 = new DirectoryTaxonomyWriter(dir2, IndexWriterConfig.OpenMode.CREATE);
    tw2.addCategory(new FacetLabel("c"));
    tw2.commit();

     // delete all files from dir1
    for (String file : dir1.listAll()) {
      System.out.println("initial file " + file);
      dir1.deleteFile(file);
    }

    // copy all index files from dir2
    for (String file : dir2.listAll()) {
      System.out.println("copying file " + file);
      dir1.copyFrom(dir2, file, file, IOContext.READ);
    }

    boolean trying = mgr.maybeRefresh();
    System.out.println("trying is " + trying);

   // System.out.println(dtr1.doOpenIfChanged());
  }

  /**
   * A modified DirectoryTaxonomyReader that always recreates a new DirectoryTaxonomyReader instance
   * when {@link AlwaysRefreshDirectoryTaxonomyReader#doOpenIfChanged()} is called. This enables us
   * to easily go forward or backward in time by re-computing the ordinal space during each refresh.
   */
  private class AlwaysRefreshDirectoryTaxonomyReader extends DirectoryTaxonomyReader {

    public AlwaysRefreshDirectoryTaxonomyReader(Directory directory) throws IOException {
      super(directory);
    }

    public AlwaysRefreshDirectoryTaxonomyReader(DirectoryReader indexReader) throws IOException {
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
