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

/**
 * Core geo-spatial types and utilities for handling geographical data and spatial operations in
 * Lucene.
 *
 * <h2>Overview</h2>
 *
 * This package provides classes for representing, indexing, and querying geographical data in
 * Lucene. It uses the WGS84 coordinate system with latitude/longitude pairs to represent points on
 * Earth.
 *
 * <h2>Core Concepts</h2>
 *
 * <ul>
 *   <li>Coordinate System: WGS84 (World Geodetic System 1984)
 *       <ul>
 *         <li>Latitude: -90° to +90° (negative for South, positive for North)
 *         <li>Longitude: -180° to +180° (negative for West, positive for East)
 *       </ul>
 *   <li>Distance calculations use haversine formula on a spherical Earth model
 *   <li>Areas near poles and the dateline receive special handling
 * </ul>
 *
 * <h2>Key Components</h2>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.geo.Point}: Represents a point on Earth's surface
 *   <li>{@link org.apache.lucene.geo.Rectangle}: Defines a bounding box for spatial queries
 *   <li>{@link org.apache.lucene.geo.Circle}: Represents a circular area with a center point and
 *       radius
 *   <li>{@link org.apache.lucene.geo.Polygon}: Represents an arbitrary polygon shape
 *   <li>{@link org.apache.lucene.geo.Line}: Represents a path or line string
 * </ul>
 *
 * <h2>Common Operations</h2>
 *
 * <ul>
 *   <li>Distance calculations between points
 *   <li>Point-in-polygon testing
 *   <li>Rectangle containment and intersection
 *   <li>Creation of bounding boxes
 *   <li>Polygon validation and preprocessing
 * </ul>
 *
 * <h2>Usage Examples</h2>
 *
 * <pre>{@code
 * Rectangle bbox = Rectangle.fromPointDistance(51.5, -0.12, 5000); // 5km around London
 * Circle circle = new Circle(48.8566, 2.3522, 1000); // 1km around Paris
 * }</pre>
 */
package org.apache.lucene.geo;
