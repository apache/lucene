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
package org.apache.lucene.document;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import java.io.IOException;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * An indexed location field for querying and sorting. If you need more fine-grained control you can
 * use {@link LatLonPoint} and {@link LatLonDocValuesField}.
 *
 * <p>Finding all documents within a range at search time is efficient. Multiple values for the same
 * field in one document is allowed.
 *
 * <p>This field defines static factory methods for common operations:
 *
 * <ul>
 *   <li>{@link #newGeometryQuery newGeometryQuery()} for matching points complying with a spatial
 *       relationship with an arbitrary geometry.
 *   <li>{@link #newDistanceFeatureQuery newDistanceFeatureQuery()} for returning points scored by
 *       distance to a specified location.
 *   <li>{@link #nearest nearest()} for returning the nearest points from a specified location.
 *   <li>{@link #newDistanceSort newDistanceSort()} for ordering documents by distance from a
 *       specified location.
 * </ul>
 *
 * <p>If you also need to store the value, you should add a separate {@link StoredField} instance.
 *
 * <p><b>WARNING</b>: Values are indexed with some loss of precision from the original {@code
 * double} values (4.190951585769653E-8 for the latitude component and 8.381903171539307E-8 for
 * longitude).
 *
 * @see LatLonPoint
 * @see LatLonDocValuesField
 */
public class LatLonField extends Field {
  /** LatLonPoint is encoded as integer values so number of bytes is 4 */
  public static final int BYTES = Integer.BYTES;
  /**
   * Type for an indexed LatLonPoint
   *
   * <p>Each point stores two dimensions with 4 bytes per dimension.
   */
  public static final FieldType TYPE = new FieldType();

  static {
    TYPE.setDimensions(2, Integer.BYTES);
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }

  // holds the doc value value.
  private long docValue;

  /**
   * Change the values of this field
   *
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if latitude or longitude are out of bounds
   */
  public void setLocationValue(double latitude, double longitude) {
    final byte[] bytes;

    if (fieldsData == null) {
      bytes = new byte[8];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef) fieldsData).bytes;
    }

    int latitudeEncoded = encodeLatitude(latitude);
    int longitudeEncoded = encodeLongitude(longitude);
    NumericUtils.intToSortableBytes(latitudeEncoded, bytes, 0);
    NumericUtils.intToSortableBytes(longitudeEncoded, bytes, Integer.BYTES);
    docValue = Long.valueOf((((long) latitudeEncoded) << 32) | (longitudeEncoded & 0xFFFFFFFFL));
  }

  @Override
  public Number numericValue() {
    return docValue;
  }

  /**
   * Creates a new LatLonPoint with the specified latitude and longitude
   *
   * @param name field name
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of
   *     bounds
   */
  public LatLonField(String name, double latitude, double longitude) {
    super(name, TYPE);
    setLocationValue(latitude, longitude);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    byte[] bytes = ((BytesRef) fieldsData).bytes;
    result.append(decodeLatitude(bytes, 0));
    result.append(',');
    result.append(decodeLongitude(bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }

  // static methods for generating queries
  public static Query newBoxQuery(
      String field,
      double minLatitude,
      double maxLatitude,
      double minLongitude,
      double maxLongitude) {
    return new IndexOrDocValuesQuery(
        LatLonPoint.newBoxQuery(field, minLatitude, maxLatitude, minLongitude, maxLongitude),
        LatLonDocValuesField.newSlowBoxQuery(
            field, minLatitude, maxLatitude, minLongitude, maxLongitude));
  }

  /**
   * Create a query for matching points within the specified distance of the supplied location.
   *
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and
   *     finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or
   *     radius is invalid.
   */
  public static Query newDistanceQuery(
      String field, double latitude, double longitude, double radiusMeters) {
    return new IndexOrDocValuesQuery(
        LatLonPoint.newDistanceQuery(field, latitude, longitude, radiusMeters),
        LatLonDocValuesField.newSlowDistanceQuery(field, latitude, longitude, radiusMeters));
  }

  /**
   * Create a query for matching one or more polygons.
   *
   * @param field field name. must not be null.
   * @param polygons array of polygons. must not be null or empty
   * @return query matching points within this polygon
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null or empty
   * @see Polygon
   */
  public static Query newPolygonQuery(String field, Polygon... polygons) {
    return new IndexOrDocValuesQuery(
        LatLonPoint.newPolygonQuery(field, polygons),
        LatLonDocValuesField.newSlowPolygonQuery(field, polygons));
  }

  /**
   * Create a query for matching one or more geometries against the provided {@link
   * ShapeField.QueryRelation}. Line geometries are not supported for WITHIN relationship.
   *
   * @param field field name. must not be null.
   * @param queryRelation The relation the points needs to satisfy with the provided geometries,
   *     must not be null.
   * @param latLonGeometries array of LatLonGeometries. must not be null or empty.
   * @return query matching points within at least one geometry.
   * @throws IllegalArgumentException if {@code field} is null, {@code queryRelation} is null,
   *     {@code latLonGeometries} is null, empty or contain a null.
   * @see LatLonGeometry
   */
  public static Query newGeometryQuery(
      String field, ShapeField.QueryRelation queryRelation, LatLonGeometry... latLonGeometries) {
    return new IndexOrDocValuesQuery(
        LatLonPoint.newGeometryQuery(field, queryRelation, latLonGeometries),
        LatLonDocValuesField.newSlowGeometryQuery(field, queryRelation, latLonGeometries));
  }

  /**
   * Given a field that indexes point values into a {@link LatLonField} and doc values into {@link
   * LatLonDocValuesField}, this returns a query that scores documents based on their haversine
   * distance in meters to {@code (originLat, originLon)}: {@code score = weight *
   * pivotDistanceMeters / (pivotDistanceMeters + distance)}, ie. score is in the {@code [0,
   * weight]} range, is equal to {@code weight} when the document's value is equal to {@code
   * (originLat, originLon)} and is equal to {@code weight/2} when the document's value is distant
   * of {@code pivotDistanceMeters} from {@code (originLat, originLon)}. In case of multi-valued
   * fields, only the closest point to {@code (originLat, originLon)} will be considered. This query
   * is typically useful to boost results based on distance by adding this query to a {@link
   * Occur#SHOULD} clause of a {@link BooleanQuery}.
   */
  public static Query newDistanceFeatureQuery(
      String field, float weight, double originLat, double originLon, double pivotDistanceMeters) {
    return LatLonPoint.newDistanceFeatureQuery(
        field, weight, originLat, originLon, pivotDistanceMeters);
  }

  /**
   * Finds the {@code n} nearest indexed points to the provided point, according to Haversine
   * distance.
   *
   * <p>This is functionally equivalent to running {@link MatchAllDocsQuery} with a {@link
   * LatLonDocValuesField#newDistanceSort}, but is far more efficient since it takes advantage of
   * properties the indexed BKD tree. Multi-valued fields are currently not de-duplicated, so if a
   * document had multiple instances of the specified field that make it into the top n, that
   * document will appear more than once.
   *
   * <p>Documents are ordered by ascending distance from the location. The value returned in {@link
   * FieldDoc} for the hits contains a Double instance with the distance in meters.
   *
   * @param searcher IndexSearcher to find nearest points from.
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param n the number of nearest neighbors to retrieve.
   * @return TopFieldDocs containing documents ordered by distance, where the field value for each
   *     {@link FieldDoc} is the distance in meters
   * @throws IllegalArgumentException if {@code field} or {@code searcher} is null, or if {@code
   *     latitude}, {@code longitude} or {@code n} are out-of-bounds
   * @throws IOException if an IOException occurs while finding the points.
   */
  // TODO: what about multi-valued documents? what happens?
  public static TopFieldDocs nearest(
      IndexSearcher searcher, String field, double latitude, double longitude, int n)
      throws IOException {
    return LatLonPoint.nearest(searcher, field, latitude, longitude, n);
  }

  /**
   * Creates a SortField for sorting by distance from a location.
   *
   * <p>This sort orders documents by ascending distance from the location. The value returned in
   * {@link FieldDoc} for the hits contains a Double instance with the distance in meters.
   *
   * <p>If a document is missing the field, then by default it is treated as having {@link
   * Double#POSITIVE_INFINITY} distance (missing values sort last).
   *
   * <p>If a document contains multiple values for the field, the <i>closest</i> distance to the
   * location is used.
   *
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or location has invalid coordinates.
   */
  public static SortField newDistanceSort(String field, double latitude, double longitude) {
    return new LatLonPointSortField(field, latitude, longitude);
  }
}
