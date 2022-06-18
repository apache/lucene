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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.facetset.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
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
    indexWriter.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("city", "city2", Field.Store.YES));
    doc.add(
        FacetSetsField.create(
            "temperature",
            new TemperatureReadingFacetSet(MAY_SECOND_2022, EIGHTY_DEGREES),
            new TemperatureReadingFacetSet(JUNE_SECOND_2022, HUNDRED_DEGREES),
            new TemperatureReadingFacetSet(JULY_SECOND_2022, HUNDRED_TWENTY_DEGREES)));
    indexWriter.addDocument(doc);

    indexWriter.close();
  }

  /** Counting documents which exactly match a given {@link FacetSet}. */
  private List<FacetResult> exactMatching() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    // Count both "Publish Date" and "Author" dimensions
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
    List<FacetResult> results = Collections.singletonList(facets.getTopChildren(10, "temperature"));

    indexReader.close();

    return results;
  }

  /** Counting documents which match a certain degrees value for any date. */
  private List<FacetResult> rangeMatching() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    // Count both "Publish Date" and "Author" dimensions
    Facets facets =
        new MatchingFacetSetsCounts(
            "temperature",
            fc,
            TemperatureReadingFacetSet::decodeTemperatureReading,
            new RangeFacetSetMatcher(
                "Eighty to Hundred Degrees",
                RangeFacetSetMatcher.fromLongs(Long.MIN_VALUE, true, Long.MAX_VALUE, true),
                RangeFacetSetMatcher.fromFloats(EIGHTY_DEGREES, true, HUNDRED_DEGREES, true)));

    // Retrieve results
    List<FacetResult> results = Collections.singletonList(facets.getTopChildren(10, "temperature"));

    indexReader.close();

    return results;
  }

  /**
   * Like {@link #rangeMatching()}, however this example demonstrates a custom {@link
   * FacetSetMatcher} which only considers certain dimensions (in this case only the temperature
   * one).
   */
  private List<FacetResult> customRangeMatching() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    // MatchAllDocsQuery is for "browsing" (counts facets
    // for all non-deleted docs in the index); normally
    // you'd use a "normal" query:
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    // Count both "Publish Date" and "Author" dimensions
    Facets facets =
        new MatchingFacetSetsCounts(
            "temperature",
            fc,
            TemperatureReadingFacetSet::decodeTemperatureReading,
            new TemperatureOnlyFacetSetMatcher(
                "Eighty to Hundred Degrees",
                RangeFacetSetMatcher.fromFloats(EIGHTY_DEGREES, true, HUNDRED_DEGREES, true)));

    // Retrieve results
    List<FacetResult> results = Collections.singletonList(facets.getTopChildren(10, "temperature"));

    indexReader.close();

    return results;
  }

  private static long date(String dateString) {
    return LocalDate.parse(dateString).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private static float fahrenheitToCelsius(int degrees) {
    return (degrees - 32.0f) * 5.f / 9.f;
  }

  /** Runs the exact matching example. */
  public List<FacetResult> runExactMatching() throws IOException {
    index();
    return exactMatching();
  }

  /** Runs the range matching example. */
  public List<FacetResult> runRangeMatching() throws IOException {
    index();
    return rangeMatching();
  }

  /** Runs the custom range matching example. */
  public List<FacetResult> runCustomRangeMatching() throws IOException {
    index();
    return customRangeMatching();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    CustomFacetSetExample example = new CustomFacetSetExample();

    System.out.println("Exact Facet Set matching example:");
    System.out.println("-----------------------");
    List<FacetResult> results = example.runExactMatching();
    System.out.println("Temperature Reading: " + results.get(0));

    System.out.println("Range Facet Set matching example:");
    System.out.println("-----------------------");
    results = example.runRangeMatching();
    System.out.println("Temperature Reading: " + results.get(0));

    System.out.println("Custom Range Facet Set matching example:");
    System.out.println("-----------------------");
    results = example.runCustomRangeMatching();
    System.out.println("Temperature Reading: " + results.get(0));
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

    private final RangeFacetSetMatcher.DimRange temperatureRange;

    /** Constructor */
    protected TemperatureOnlyFacetSetMatcher(
        String label, RangeFacetSetMatcher.DimRange temperatureRange) {
      super(label, 1); // We only evaluate one dimension

      this.temperatureRange = temperatureRange;
    }

    @Override
    public boolean matches(long[] dimValues) {
      return temperatureRange.min <= dimValues[1] && temperatureRange.max >= dimValues[1];
    }
  }
}
