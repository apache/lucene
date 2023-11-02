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

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.facetset.DimRange;
import org.apache.lucene.facet.facetset.ExactFacetSetMatcher;
import org.apache.lucene.facet.facetset.FacetSet;
import org.apache.lucene.facet.facetset.FacetSetDecoder;
import org.apache.lucene.facet.facetset.FacetSetMatcher;
import org.apache.lucene.facet.facetset.FacetSetsField;
import org.apache.lucene.facet.facetset.MatchingFacetSetsCounts;
import org.apache.lucene.facet.facetset.RangeFacetSetMatcher;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Shows usage of indexing and searching {@link FacetSetsField} with a custom {@link FacetSet}
 * implementation. Unlike the out of the box {@link FacetSet} implementations, this example shows
 * how to mix and match dimensions of different types, as well as implementing a custom {@link
 * FacetSetMatcher}.
 */
public class CustomFacetSetExample {

  private static final long MAY_SECOND_2022 = date("2022-05-02");
  private static final long JUNE_SECOND_2022 = date("2022-06-02");
  private static final long JULY_SECOND_2022 = date("2022-07-02");
  private static final float HUNDRED_TWENTY_DEGREES = fahrenheitToCelsius(120);
  private static final float HUNDRED_DEGREES = fahrenheitToCelsius(100);
  private static final float EIGHTY_DEGREES = fahrenheitToCelsius(80);

  private final Directory indexDir = new ByteBuffersDirectory();

  /** Empty constructor */
  public CustomFacetSetExample() {}

  /** Build the example index. */
  private void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // Every document holds the temperature measures for a City by Date

    Document doc = new Document();
    doc.add(new StringField("city", "city1", Field.Store.YES));
    doc.add(
        FacetSetsField.create(
            "temperature",
            new TemperatureReadingFacetSet(MAY_SECOND_2022, HUNDRED_DEGREES),
            new TemperatureReadingFacetSet(JUNE_SECOND_2022, EIGHTY_DEGREES),
            new TemperatureReadingFacetSet(JULY_SECOND_2022, HUNDRED_TWENTY_DEGREES)));
    addFastMatchFields(doc);
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("city", "city2", Field.Store.YES));
    doc.add(
        FacetSetsField.create(
            "temperature",
            new TemperatureReadingFacetSet(MAY_SECOND_2022, EIGHTY_DEGREES),
            new TemperatureReadingFacetSet(JUNE_SECOND_2022, HUNDRED_DEGREES),
            new TemperatureReadingFacetSet(JULY_SECOND_2022, HUNDRED_TWENTY_DEGREES)));
    addFastMatchFields(doc);
    indexWriter.addDocument(doc);

    indexWriter.close();
  }

  private void addFastMatchFields(Document doc) {
    // day field
    doc.add(new StringField("day", String.valueOf(MAY_SECOND_2022), Field.Store.NO));
    doc.add(new StringField("day", String.valueOf(JUNE_SECOND_2022), Field.Store.NO));
    doc.add(new StringField("day", String.valueOf(JULY_SECOND_2022), Field.Store.NO));

    // temp field
    doc.add(new StringField("temp", String.valueOf(EIGHTY_DEGREES), Field.Store.NO));
    doc.add(new StringField("temp", String.valueOf(HUNDRED_DEGREES), Field.Store.NO));
    doc.add(new StringField("temp", String.valueOf(HUNDRED_TWENTY_DEGREES), Field.Store.NO));
  }

  /** Counting documents which exactly match a given {@link FacetSet}. */
  private FacetResult exactMatching() throws IOException {
    try (DirectoryReader indexReader = DirectoryReader.open(indexDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);

      // MatchAllDocsQuery is for "browsing" (counts facets
      // for all non-deleted docs in the index); normally
      // you'd use a "normal" query:
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

      // Count both "May 2022, 100 degrees" and "July 2022, 120 degrees" dimensions
      Facets facets =
          new MatchingFacetSetsCounts(
              "temperature",
              fc,
              TemperatureReadingFacetSet::decodeTemperatureReading,
              new ExactFacetSetMatcher(
                  "May 2022 (100f)",
                  new TemperatureReadingFacetSet(MAY_SECOND_2022, HUNDRED_DEGREES)),
              new ExactFacetSetMatcher(
                  "July 2022 (120f)",
                  new TemperatureReadingFacetSet(JULY_SECOND_2022, HUNDRED_TWENTY_DEGREES)));

      // Retrieve results
      return facets.getAllChildren("temperature");
    }
  }

  /**
   * Counting documents which exactly match a given {@link FacetSet}. This example also demonstrates
   * how to use a fast match query to improve the counting efficiency by skipping over documents
   * which cannot possibly match a set.
   */
  private FacetResult exactMatchingWithFastMatchQuery() throws IOException {
    try (DirectoryReader indexReader = DirectoryReader.open(indexDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);

      // MatchAllDocsQuery is for "browsing" (counts facets
      // for all non-deleted docs in the index); normally
      // you'd use a "normal" query:
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

      // Match documents whose "day" field is either "May 2022" or "July 2022"
      Query dateQuery =
          new TermInSetQuery(
              "day",
              Arrays.asList(
                  new BytesRef(String.valueOf(MAY_SECOND_2022)),
                  new BytesRef(String.valueOf(JULY_SECOND_2022))));
      // Match documents whose "temp" field is either "80" or "120" degrees
      Query temperatureQuery =
          new TermInSetQuery(
              "temp",
              Arrays.asList(
                  new BytesRef(String.valueOf(HUNDRED_DEGREES)),
                  new BytesRef(String.valueOf(HUNDRED_TWENTY_DEGREES))));
      // Documents must match both clauses
      Query fastMatchQuery =
          new BooleanQuery.Builder()
              .add(dateQuery, BooleanClause.Occur.MUST)
              .add(temperatureQuery, BooleanClause.Occur.MUST)
              .build();

      // Count both "May 2022, 100 degrees" and "July 2022, 120 degrees" dimensions
      Facets facets =
          new MatchingFacetSetsCounts(
              "temperature",
              fc,
              TemperatureReadingFacetSet::decodeTemperatureReading,
              fastMatchQuery,
              new ExactFacetSetMatcher(
                  "May 2022 (100f)",
                  new TemperatureReadingFacetSet(MAY_SECOND_2022, HUNDRED_DEGREES)),
              new ExactFacetSetMatcher(
                  "July 2022 (120f)",
                  new TemperatureReadingFacetSet(JULY_SECOND_2022, HUNDRED_TWENTY_DEGREES)));

      // Retrieve results
      return facets.getAllChildren("temperature");
    }
  }

  /** Counting documents which match a certain degrees value for any date. */
  private FacetResult rangeMatching() throws IOException {
    try (DirectoryReader indexReader = DirectoryReader.open(indexDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);

      // MatchAllDocsQuery is for "browsing" (counts facets
      // for all non-deleted docs in the index); normally
      // you'd use a "normal" query:
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

      // Count 80-100 degrees
      Facets facets =
          new MatchingFacetSetsCounts(
              "temperature",
              fc,
              TemperatureReadingFacetSet::decodeTemperatureReading,
              new RangeFacetSetMatcher(
                  "Eighty to Hundred Degrees",
                  DimRange.fromLongs(Long.MIN_VALUE, true, Long.MAX_VALUE, true),
                  DimRange.fromFloats(EIGHTY_DEGREES, true, HUNDRED_DEGREES, true)));

      // Retrieve results
      return facets.getAllChildren("temperature");
    }
  }

  /**
   * Like {@link #rangeMatching()}, however this example demonstrates a custom {@link
   * FacetSetMatcher} which only considers certain dimensions (in this case only the temperature
   * one).
   */
  private FacetResult customRangeMatching() throws IOException {
    try (DirectoryReader indexReader = DirectoryReader.open(indexDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);

      // MatchAllDocsQuery is for "browsing" (counts facets
      // for all non-deleted docs in the index); normally
      // you'd use a "normal" query:
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

      // Count 80-100 degrees
      Facets facets =
          new MatchingFacetSetsCounts(
              "temperature",
              fc,
              TemperatureReadingFacetSet::decodeTemperatureReading,
              new TemperatureOnlyFacetSetMatcher(
                  "Eighty to Hundred Degrees",
                  DimRange.fromFloats(EIGHTY_DEGREES, true, HUNDRED_DEGREES, true)));

      // Retrieve results
      return facets.getAllChildren("temperature");
    }
  }

  private static long date(String dateString) {
    return LocalDate.parse(dateString).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private static float fahrenheitToCelsius(int degrees) {
    return (degrees - 32.0f) * 5.f / 9.f;
  }

  /** Runs the exact matching example. */
  public FacetResult runExactMatching() throws IOException {
    index();
    return exactMatching();
  }

  /** Runs the exact matching with fast match query example. */
  public FacetResult runExactMatchingWithFastMatchQuery() throws IOException {
    index();
    return exactMatchingWithFastMatchQuery();
  }

  /** Runs the range matching example. */
  public FacetResult runRangeMatching() throws IOException {
    index();
    return rangeMatching();
  }

  /** Runs the custom range matching example. */
  public FacetResult runCustomRangeMatching() throws IOException {
    index();
    return customRangeMatching();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    CustomFacetSetExample example = new CustomFacetSetExample();

    System.out.println("Exact Facet Set matching example:");
    System.out.println("-----------------------");
    FacetResult result = example.runExactMatching();
    System.out.println("Temperature Reading: " + result);

    System.out.println("Exact Facet Set matching with fast match query example:");
    System.out.println("-----------------------");
    result = example.runExactMatchingWithFastMatchQuery();
    System.out.println("Temperature Reading: " + result);

    System.out.println("Range Facet Set matching example:");
    System.out.println("-----------------------");
    result = example.runRangeMatching();
    System.out.println("Temperature Reading: " + result);

    System.out.println("Custom Range Facet Set matching example:");
    System.out.println("-----------------------");
    result = example.runCustomRangeMatching();
    System.out.println("Temperature Reading: " + result);
  }

  /**
   * A {@link FacetSet} which encodes a temperature reading in a date (long) and degrees (celsius;
   * float).
   */
  public static class TemperatureReadingFacetSet extends FacetSet {

    private static final int SIZE_PACKED_BYTES = Long.BYTES + Float.BYTES;

    private final long date;
    private final float degrees;

    /** Constructor */
    public TemperatureReadingFacetSet(long date, float degrees) {
      super(2); // We encode two dimensions

      this.date = date;
      this.degrees = degrees;
    }

    @Override
    public long[] getComparableValues() {
      return new long[] {date, NumericUtils.floatToSortableInt(degrees)};
    }

    @Override
    public int packValues(byte[] buf, int start) {
      LongPoint.encodeDimension(date, buf, start);
      // Encode 'degrees' as a sortable integer.
      FloatPoint.encodeDimension(degrees, buf, start + Long.BYTES);
      return sizePackedBytes();
    }

    @Override
    public int sizePackedBytes() {
      return SIZE_PACKED_BYTES;
    }

    /**
     * An implementation of {@link FacetSetDecoder#decode(BytesRef, int, long[])} for {@link
     * TemperatureReadingFacetSet}.
     */
    public static int decodeTemperatureReading(BytesRef bytesRef, int start, long[] dest) {
      dest[0] = LongPoint.decodeDimension(bytesRef.bytes, start);
      // Decode the degrees as a sortable integer.
      dest[1] = IntPoint.decodeDimension(bytesRef.bytes, start + Long.BYTES);
      return SIZE_PACKED_BYTES;
    }
  }

  /**
   * A {@link FacetSetMatcher} which matches facet sets only by their temperature dimension,
   * ignoring the date.
   */
  public static class TemperatureOnlyFacetSetMatcher extends FacetSetMatcher {

    private final DimRange temperatureRange;

    /** Constructor */
    protected TemperatureOnlyFacetSetMatcher(String label, DimRange temperatureRange) {
      super(label, 1); // We only evaluate one dimension

      this.temperatureRange = temperatureRange;
    }

    @Override
    public boolean matches(long[] dimValues) {
      return temperatureRange.min <= dimValues[1] && temperatureRange.max >= dimValues[1];
    }
  }
}
