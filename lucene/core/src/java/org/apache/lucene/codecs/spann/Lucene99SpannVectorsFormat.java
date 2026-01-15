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
package org.apache.lucene.codecs.spann;

import java.io.IOException;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
* Lucene 9.9 SPANN Vectors Format.
*
* <p>This format implements the <b>Disk-Resident HNSW-IVF</b> architecture. It is a composite codec
* that delegates:
*
* <ul>
*   <li><b>Navigation</b>: To {@link Lucene99HnswVectorsFormat} (Off-Heap Centroids).
*   <li><b>Storage</b>: To a local sequential implementation (Disk-Resident Data).
* </ul>
*/
public final class Lucene99SpannVectorsFormat extends KnnVectorsFormat {

 public static final String FORMAT_NAME = "Lucene99SpannVectors";

 /**
 * We delegate the "Centroid Index" to HNSW. This allows us to use off-heap navigation for
 * millions of centroids.
 */
 private final KnnVectorsFormat centroidIndexFormat;

 public Lucene99SpannVectorsFormat() {
  super(FORMAT_NAME);
  this.centroidIndexFormat = new Lucene99HnswVectorsFormat();
 }

 @Override
 public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
  return new Lucene99SpannVectorsWriter(state, centroidIndexFormat);
 }

 @Override
 public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
  return new Lucene99SpannVectorsReader(state, centroidIndexFormat);
 }

 @Override
 public int getMaxDimensions(String fieldName) {
  return 1024; // SPANN limit
 }
}
