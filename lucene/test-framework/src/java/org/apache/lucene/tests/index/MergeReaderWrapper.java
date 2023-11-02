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

package org.apache.lucene.tests.index;

import java.io.IOException;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
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

/**
 * This is a hack to make index sorting fast, with a {@link LeafReader} that always returns merge
 * instances when you ask for the codec readers.
 */
class MergeReaderWrapper extends LeafReader {
  final CodecReader in;
  final FieldsProducer fields;
  final NormsProducer norms;
  final DocValuesProducer docValues;
  final StoredFieldsReader store;
  final TermVectorsReader vectors;

  MergeReaderWrapper(CodecReader in) throws IOException {
    this.in = in;

    FieldsProducer fields = in.getPostingsReader();
    if (fields != null) {
      fields = fields.getMergeInstance();
    }
    this.fields = fields;

    NormsProducer norms = in.getNormsReader();
    if (norms != null) {
      norms = norms.getMergeInstance();
    }
    this.norms = norms;

    DocValuesProducer docValues = in.getDocValuesReader();
    if (docValues != null) {
      docValues = docValues.getMergeInstance();
    }
    this.docValues = docValues;

    StoredFieldsReader store = in.getFieldsReader();
    if (store != null) {
      store = store.getMergeInstance();
    }
    this.store = store;

    TermVectorsReader vectors = in.getTermVectorsReader();
    if (vectors != null) {
      vectors = vectors.getMergeInstance();
    }
    this.vectors = vectors;
  }

  @Override
  public Terms terms(String field) throws IOException {
    ensureOpen();
    // We could check the FieldInfo IndexOptions but there's no point since
    //   PostingsReader will simply return null for fields that don't exist or that have no terms
    // index.
    if (fields == null) {
      return null;
    }
    return fields.terms(field);
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.NUMERIC) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getNumeric(fi);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.BINARY) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getBinary(fi);
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSorted(fi);
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSortedNumeric(fi);
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED_SET) {
      // Field was not indexed with doc values
      return null;
    }
    return docValues.getSortedSet(fi);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    FieldInfo fi = getFieldInfos().fieldInfo(field);
    if (fi == null || !fi.hasNorms()) {
      // Field does not exist or does not index norms
      return null;
    }
    return norms.getNorms(fi);
  }

  @Override
  public FieldInfos getFieldInfos() {
    return in.getFieldInfos();
  }

  @Override
  public Bits getLiveDocs() {
    return in.getLiveDocs();
  }

  @Override
  public void checkIntegrity() throws IOException {
    in.checkIntegrity();
  }

  @Override
  public TermVectors termVectors() throws IOException {
    ensureOpen();
    if (vectors == null) {
      return TermVectors.EMPTY;
    } else {
      return vectors;
    }
  }

  @Override
  public PointValues getPointValues(String fieldName) throws IOException {
    return in.getPointValues(fieldName);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String fieldName) throws IOException {
    return in.getFloatVectorValues(fieldName);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String fieldName) throws IOException {
    return in.getByteVectorValues(fieldName);
  }

  @Override
  public void searchNearestVectors(
      String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void searchNearestVectors(
      String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  @Override
  public int numDocs() {
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    return in.maxDoc();
  }

  @Override
  public StoredFields storedFields() throws IOException {
    ensureOpen();
    return store;
  }

  @Override
  protected void doClose() throws IOException {
    in.close();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return in.getCoreCacheHelper();
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public String toString() {
    return "MergeReaderWrapper(" + in + ")";
  }

  @Override
  public LeafMetaData getMetaData() {
    return in.getMetaData();
  }
}
