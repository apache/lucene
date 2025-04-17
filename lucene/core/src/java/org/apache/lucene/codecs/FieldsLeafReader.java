/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

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
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Map;

/** Minimal LeafReader exposing term statistics for one or more fields. */
public final class FieldsLeafReader extends LeafReader {

  private final Map<String, Terms> fieldTerms;
  private final int maxDoc;

  /**
   * Creates a {@code FieldsLeafReader} for the given field terms.
   *
   * @param fieldTerms mapping of field names to {@link Terms} enumerations
   * @param maxDoc total number of documents in this reader
   */
  public FieldsLeafReader(Map<String, Terms> fieldTerms, int maxDoc) {
    this.fieldTerms = fieldTerms;
    this.maxDoc = maxDoc;
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public Terms terms(String field) throws IOException {
    return fieldTerms.get(field);
  }

  @Override
  public int maxDoc() {
    return maxDoc;
  }

  @Override
  public StoredFields storedFields() throws IOException {
    return null;
  }

  @Override
  public TermVectors termVectors() throws IOException {
    return null;
  }

  @Override
  public int numDocs() {
    return maxDoc;
  }

  @Override
  public Bits getLiveDocs() {
    return null;
  }

  @Override
  public FieldInfos getFieldInfos() {
    throw new UnsupportedOperationException("field infos not supported");
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) {
    return null;
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    return null;
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) {
    return null;
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) {
    return null;
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) {
    return null;
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    return null;
  }

  @Override
  public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
    return null;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return null;
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return null;
  }

  @Override
  public void searchNearestVectors(
      String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {}

  @Override
  public void searchNearestVectors(
      String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {}

  @Override
  public PointValues getPointValues(String field) {
    return null;
  }

  @Override
  public LeafMetaData getMetaData() {
    return new LeafMetaData(Version.LATEST.major, Version.LATEST, null, false);
  }

  @Override
  public void checkIntegrity() {}

  @Override
  protected void doClose() {}

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }
}
