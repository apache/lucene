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

package org.apache.lucene.monitor;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class TestDocumentBatch extends LuceneTestCase {

  public static final Analyzer ANALYZER = new StandardAnalyzer();

  @Test(expected = IllegalArgumentException.class)
  public void testDocumentBatchThrowsIllegalArgumentExceptionUponZeroDocument() {
    DocumentBatch.of(ANALYZER);
  }

  public void testSingleDocumentAndArrayOfOneDocumentResultInSameDocumentBatch()
      throws IOException {
    Document doc = new Document();
    try (DocumentBatch batchDoc = DocumentBatch.of(ANALYZER, doc);
        DocumentBatch batchArr = DocumentBatch.of(ANALYZER, new Document[] {doc})) {
      MatcherAssert.assertThat(
          batchDoc.getClass().getName(), containsString("SingletonDocumentBatch"));
      assertEquals(batchDoc.getClass(), batchArr.getClass());
    }
  }

  public void testDocumentBatchClassDiffersWhetherItContainsOneOrMoreDocuments()
      throws IOException {
    Document doc = new Document();
    try (DocumentBatch batch1 = DocumentBatch.of(ANALYZER, new Document[] {doc});
        DocumentBatch batch2 = DocumentBatch.of(ANALYZER, doc, doc);
        DocumentBatch batch3 = DocumentBatch.of(ANALYZER, doc, doc, doc)) {
      assertNotEquals(batch1.getClass(), batch2.getClass());
      assertEquals(batch2.getClass(), batch3.getClass());
      MatcherAssert.assertThat(batch3.getClass().getName(), containsString("MultiDocumentBatch"));
    }
  }
}
