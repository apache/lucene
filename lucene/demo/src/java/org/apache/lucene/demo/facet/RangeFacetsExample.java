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
package org.apache.lucene.demo.facet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/** Shows simple usage of dynamic range faceting. */
public class RangeFacetsExample implements Closeable {

  private final Directory indexDir = new ByteBuffersDirectory();
  private IndexSearcher searcher;
  private LongRange[] logTimestampRanges = new LongRange[168];
  private final long nowSec = System.currentTimeMillis() / 1000L;

  final LongRange PAST_HOUR = new LongRange("Past hour", nowSec - 3600, true, nowSec, true);
  final LongRange PAST_SIX_HOURS =
      new LongRange("Past six hours", nowSec - 6 * 3600, true, nowSec, true);
  final LongRange PAST_DAY = new LongRange("Past day", nowSec - 24 * 3600, true, nowSec, true);

  /** Empty constructor */
  public RangeFacetsExample() {}

  /** Build the example index. */
  public void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // Add documents with a fake timestamp, 1000 sec before
    // "now", 2000 sec before "now", ...:
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      long then = nowSec - i * 1000L;
      // Add as doc values field, so we can compute range facets:
      doc.add(new NumericDocValuesField("timestamp", then));
      // Add as numeric field so we can drill-down:
      doc.add(new LongPoint("timestamp", then));
      indexWriter.addDocument(doc);
    }

    // Add documents with a fake timestamp for the past 7 days (24 * 7 = 168 hours), 3600 sec (1
    // hour) from "now", 7200 sec (2 hours) from "now", ...:
    long startTime = 0;
    for (int i = 0; i < 168; i++) {
      long endTime = (i + 1) * 3600L;
      // Choose a relatively large number, e,g., "35", to create variation in count for
      // the top n children, so that calling getTopChildren(10) can return top 10 children with
      // different counts
      for (int j = 0; j < i % 35; j++) {
        Document doc = new Document();
        Random r = new Random();
        // Randomly generate a timestamp within the current range
        long randomTimestamp = r.nextLong(1, endTime - startTime) + startTime;
        // Add as doc values field, so we can compute range facets:
        doc.add(new NumericDocValuesField("error timestamp", randomTimestamp));
        doc.add(
            new StringField(
                "error message", "server encountered error at " + randomTimestamp, Field.Store.NO));
        indexWriter.addDocument(doc);
      }
      logTimestampRanges[i] =
          new LongRange("Hour " + i + "-" + (i + 1), startTime, false, endTime, true);
      startTime = endTime;
    }

    // Open near-real-time searcher
    searcher = new IndexSearcher(DirectoryReader.open(indexWriter));
    indexWriter.close();
  }

  private FacetsConfig getConfig() {
    return new FacetsConfig();
  }

  /** User runs a query and counts facets. */
  public FacetResult search() throws IOException {

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc =
        FacetsCollectorManager.search(
                searcher, MatchAllDocsQuery.INSTANCE, 10, new FacetsCollectorManager())
            .facetsCollector();

    Facets facets = new LongRangeFacetCounts("timestamp", fc, PAST_HOUR, PAST_SIX_HOURS, PAST_DAY);
    return facets.getAllChildren("timestamp");
  }

  /** User runs a query and counts facets. */
  public FacetResult searchTopChildren() throws IOException {

    // Aggregates the facet counts
    FacetsCollectorManager fcm = new FacetsCollectorManager();

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc =
        FacetsCollectorManager.search(searcher, MatchAllDocsQuery.INSTANCE, 10, fcm)
            .facetsCollector();

    Facets facets = new LongRangeFacetCounts("error timestamp", fc, logTimestampRanges);
    return facets.getTopChildren(10, "error timestamp");
  }

  /** User drills down on the specified range. */
  public TopDocs drillDown(LongRange range) throws IOException {

    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(getConfig());

    q.add("timestamp", LongPoint.newRangeQuery("timestamp", range.min, range.max));
    return searcher.search(q, 10);
  }

  /** User drills down on the specified range, and also computes drill sideways counts. */
  public DrillSideways.DrillSidewaysResult drillSideways(LongRange range) throws IOException {
    // Passing no baseQuery means we drill down on all
    // documents ("browse only"):
    DrillDownQuery q = new DrillDownQuery(getConfig());
    q.add("timestamp", LongPoint.newRangeQuery("timestamp", range.min, range.max));

    // DrillSideways only handles taxonomy and sorted set drill facets by default; to do range
    // facets we must subclass and override the
    // buildFacetsResult method.
    DrillSideways.DrillSidewaysResult result =
        new DrillSideways(searcher, getConfig(), null, null) {
          @Override
          protected Facets buildFacetsResult(
              FacetsCollector drillDowns,
              FacetsCollector[] drillSideways,
              String[] drillSidewaysDims)
              throws IOException {
            // If we had other dims we would also compute their drill-down or drill-sideways facets
            // here:
            assert drillSidewaysDims[0].equals("timestamp");
            return new LongRangeFacetCounts(
                "timestamp", drillSideways[0], PAST_HOUR, PAST_SIX_HOURS, PAST_DAY);
          }
        }.search(q, 10);

    return result;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(searcher.getIndexReader(), indexDir);
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();

    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    System.out.println(example.search());

    System.out.println("\n");
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    System.out.println(example.searchTopChildren());

    System.out.println("\n");
    System.out.println("Facet drill-down example (timestamp/Past six hours):");
    System.out.println("---------------------------------------------");
    TopDocs hits = example.drillDown(example.PAST_SIX_HOURS);
    System.out.println(hits.totalHits + " totalHits");

    System.out.println("\n");
    System.out.println("Facet drill-sideways example (timestamp/Past six hours):");
    System.out.println("---------------------------------------------");
    DrillSideways.DrillSidewaysResult sideways = example.drillSideways(example.PAST_SIX_HOURS);
    System.out.println(sideways.hits.totalHits + " totalHits");
    System.out.println(sideways.facets.getTopChildren(10, "timestamp"));

    example.close();
  }
}
