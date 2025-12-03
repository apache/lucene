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

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFilterCodecReader extends LuceneTestCase {

  public void testDeclaredMethodsOverridden() throws Exception {
    assertDelegatorOverridesAllRequiredMethods(FilterCodecReader.class, Set.of());
  }

  public void testGetDelegate() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      w.addDocument(new Document());
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        FilterCodecReader r =
            FilterCodecReader.wrapLiveDocs(
                (CodecReader) reader.getSequentialSubReaders().get(0), null, 1);

        assertSame(FilterCodecReader.unwrap(r), reader.getSequentialSubReaders().get(0));
        assertSame(r.getDelegate(), reader.getSequentialSubReaders().get(0));
      }
    }
  }
}
