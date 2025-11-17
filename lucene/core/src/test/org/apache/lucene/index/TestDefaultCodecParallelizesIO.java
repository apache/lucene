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
package org.apache.lucene.index;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.SerialIOCountingDirectory;
import org.apache.lucene.tests.util.LineFileDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestDefaultCodecParallelizesIO extends LuceneTestCase {

  private static SerialIOCountingDirectory dir;
  private static IndexReader reader;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Directory bbDir = new ByteBuffersDirectory();
    try (LineFileDocs docs = new LineFileDocs(random());
        IndexWriter w =
            new IndexWriter(
                bbDir,
                new IndexWriterConfig()
                    .setUseCompoundFile(false)
                    .setMergePolicy(newLogMergePolicy(false))
                    .setCodec(TestUtil.getDefaultCodec()))) {
      final int numDocs = atLeast(10_000);
      for (int d = 0; d < numDocs; ++d) {
        Document doc = docs.nextDoc();
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
    dir = new SerialIOCountingDirectory(bbDir);
    reader = DirectoryReader.open(dir);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.close(reader, dir);
  }

  /** Simulate term lookup in a BooleanQuery. */
  public void testTermsSeekExact() throws IOException {
    long prevCount = dir.count();

    Terms terms = getOnlyLeafReader(reader).terms("body");
    String[] termValues = new String[] {"a", "which", "the", "for", "he"};
    IOBooleanSupplier[] suppliers = new IOBooleanSupplier[termValues.length];
    for (int i = 0; i < termValues.length; ++i) {
      TermsEnum te = terms.iterator();
      suppliers[i] = te.prepareSeekExact(new BytesRef(termValues[i]));
    }
    int nonNullIOSuppliers = 0;
    for (IOBooleanSupplier supplier : suppliers) {
      if (supplier != null) {
        nonNullIOSuppliers++;
        supplier.get();
      }
    }

    assertThat(nonNullIOSuppliers, greaterThan(0));
    long newCount = dir.count();
    assertThat(newCount, greaterThan(prevCount));
    assertThat(newCount, lessThan(prevCount + nonNullIOSuppliers));
  }

  /** Simulate stored fields retrieval. */
  public void testStoredFields() throws IOException {
    long prevCount = dir.count();

    LeafReader leafReader = getOnlyLeafReader(reader);
    StoredFields storedFields = leafReader.storedFields();
    int[] docs = new int[20];
    for (int i = 0; i < docs.length; ++i) {
      docs[i] = random().nextInt(leafReader.maxDoc());
      storedFields.prefetch(docs[i]);
    }
    for (int doc : docs) {
      storedFields.document(doc);
    }

    long newCount = dir.count();
    assertThat(newCount, greaterThan(prevCount));
    assertThat(newCount, lessThan(prevCount + docs.length));
  }
}
