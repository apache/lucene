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
package org.apache.lucene.facet.facetset;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;

public class TestMatchingFacetSetsCounts extends FacetTestCase {

  public void testInvalidTopN() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    Document doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123, 456)));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new MatchingFacetSetsCounts(
            "field",
            fc,
            FacetSetDecoder::decodeLongs,
            new ExactFacetSetMatcher("Test", new LongFacetSet(123, 456)));

    expectThrows(IllegalArgumentException.class, () -> facets.getTopChildren(0, "field"));

    r.close();
    d.close();
  }

  public void testInconsistentNumOfIndexedDimensions() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    Document doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123, 456)));
    w.addDocument(doc);

    doc = new Document();
    doc.add(FacetSetsField.create("field", new LongFacetSet(123)));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    expectThrows(
        AssertionError.class,
        () ->
            new MatchingFacetSetsCounts(
                "field",
                fc,
                FacetSetDecoder::decodeLongs,
                new ExactFacetSetMatcher("Test", new LongFacetSet(1))));

    r.close();
    d.close();
  }
}
