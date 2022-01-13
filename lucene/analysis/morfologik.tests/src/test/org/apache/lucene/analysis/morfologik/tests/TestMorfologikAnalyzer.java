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
package org.apache.lucene.analysis.morfologik.tests;

import org.apache.lucene.analysis.morfologik.MorfologikAnalyzer;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;

public class TestMorfologikAnalyzer extends LuceneTestCase {
  public void testMorfologikAnalyzerLoads() {
    var analyzer = new MorfologikAnalyzer();
    Assert.assertNotNull(analyzer);
  }

  public void testUkrainianMorfologikAnalyzerLoads() {
    var analyzer = new UkrainianMorfologikAnalyzer();
    Assert.assertNotNull(analyzer);
  }

  public void testWeAreModule() {
    Assert.assertTrue(this.getClass().getModule().isNamed());
  }

  public void testLuceneIsAModule() {
    Assert.assertTrue(IndexWriter.class.getModule().isNamed());
  }
}
