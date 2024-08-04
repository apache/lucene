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

package org.apache.lucene.monitor.outsidepackage;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.monitor.CandidateMatcher;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.junit.Test;

public class TestCandidateMatcherVisibility {

  // Dummy empty IndexReader for use in creating a matcher
  private LeafReader dummyIndexReader() {
    return new LeafReader() {
      @Override
      public int maxDoc() {
        return 0;
      }

      @Override
      public int numDocs() {
        return 0;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return FieldInfos.EMPTY;
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public Terms terms(String field) throws IOException {
        return null;
      }

      @Override
      public TermVectors termVectors() {
        return TermVectors.EMPTY;
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) {
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) {
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) {
        return null;
      }

      @Override
      public DocValuesSkipper getDocValuesSkipper(String field) {
        return null;
      }

      @Override
      public PointValues getPointValues(String field) {
        return null;
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) {
        return null;
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) {
        return null;
      }

      @Override
      public void searchNearestVectors(
          String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      public void searchNearestVectors(
          String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {}

      @Override
      protected void doClose() {}

      @Override
      public StoredFields storedFields() {
        return new StoredFields() {
          @Override
          public void document(int doc, StoredFieldVisitor visitor) {}
        };
      }

      @Override
      public void checkIntegrity() throws IOException {}

      @Override
      public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  private CandidateMatcher<QueryMatch> newCandidateMatcher() {
    // Dummy searcher for use in creating a matcher
    final IndexSearcher mockSearcher = new IndexSearcher(dummyIndexReader());
    return QueryMatch.SIMPLE_MATCHER.createMatcher(mockSearcher);
  }

  @Test
  public void testMatchQueryVisibleOutsidePackage() throws IOException {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.matchQuery("test", new TermQuery(new Term("test_field")), Collections.emptyMap());
  }

  @Test
  public void testReportErrorVisibleOutsidePackage() {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.reportError("test", new RuntimeException("test exception"));
  }

  @Test
  public void testFinishVisibleOutsidePackage() {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.finish(0, 0);
  }
}
