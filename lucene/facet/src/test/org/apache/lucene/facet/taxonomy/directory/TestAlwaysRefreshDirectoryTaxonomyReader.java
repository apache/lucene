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
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

public class TestAlwaysRefreshDirectoryTaxonomyReader extends FacetTestCase {

  @Test
  /**
   * Tests the expert constructors in {@link DirectoryTaxonomyReader} and checks the {@link
   * DirectoryTaxonomyReader#getInternalIndexReader()} API. Also demonstrates the need for the
   * constructor and the API.
   *
   * <p>It does not check whether the private taxoArrays were actually recreated or no. We are
   * (correctly) hiding away that complexity away from the user.
   */
  public void testAlwaysRefreshDirectoryTaxonomyReader() throws IOException {
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dir);
    tw.addCategory(new FacetLabel("a"));
    tw.commit();

    DirectoryTaxonomyReader dtr1 = new AlwaysRefreshDirectoryTaxonomyReader(dir);
    tw.addCategory(new FacetLabel("b"));
    tw.commit();
    DirectoryTaxonomyReader dtr2 = dtr1.doOpenIfChanged();

    assert dtr2.getSize() == 3; // one doc is by default for the root ordinal
    IOUtils.close(tw, dtr1, dtr2, dir);
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
