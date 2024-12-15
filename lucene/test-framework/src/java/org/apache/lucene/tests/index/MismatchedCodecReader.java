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
import java.util.Objects;
import java.util.Random;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * Shuffles field numbers around to try to trip bugs where field numbers are assumed to always be
 * consistent across segments.
 */
public class MismatchedCodecReader extends FilterCodecReader {

  private final FieldInfos shuffled;

  /** Sole constructor. */
  public MismatchedCodecReader(CodecReader in, Random random) {
    super(in);
    shuffled = MismatchedLeafReader.shuffleInfos(in.getFieldInfos(), random);
  }

  @Override
  public FieldInfos getFieldInfos() {
    return shuffled;
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
  public StoredFieldsReader getFieldsReader() {
    StoredFieldsReader in = super.getFieldsReader();
    if (in == null) {
      return null;
    }
    return new MismatchedStoredFieldsReader(in, shuffled);
  }

  private static class MismatchedStoredFieldsReader extends StoredFieldsReader {

    private final StoredFieldsReader in;
    private final FieldInfos shuffled;

    MismatchedStoredFieldsReader(StoredFieldsReader in, FieldInfos shuffled) {
      this.in = Objects.requireNonNull(in);
      this.shuffled = shuffled;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public StoredFieldsReader clone() {
      return new MismatchedStoredFieldsReader(in.clone(), shuffled);
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
      in.document(docID, new MismatchedLeafReader.MismatchedVisitor(visitor, shuffled));
    }
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    DocValuesProducer in = super.getDocValuesReader();
    if (in == null) {
      return null;
    }
    return new MismatchedDocValuesProducer(in, shuffled, super.getFieldInfos());
  }

  private static class MismatchedDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer in;
    private final FieldInfos shuffled;
    private final FieldInfos orig;

    MismatchedDocValuesProducer(DocValuesProducer in, FieldInfos shuffled, FieldInfos orig) {
      this.in = Objects.requireNonNull(in);
      this.shuffled = shuffled;
      this.orig = orig;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    private FieldInfo remapFieldInfo(FieldInfo field) {
      FieldInfo fi = shuffled.fieldInfo(field.name);
      assert fi != null && fi.number == field.number;
      return orig.fieldInfo(field.name);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
      return in.getNumeric(remapFieldInfo(field));
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
      return in.getBinary(remapFieldInfo(field));
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
      return in.getSorted(remapFieldInfo(field));
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
      return in.getSortedNumeric(remapFieldInfo(field));
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
      return in.getSortedSet(remapFieldInfo(field));
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
      return in.getSkipper(remapFieldInfo(field));
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
  }

  @Override
  public NormsProducer getNormsReader() {
    NormsProducer in = super.getNormsReader();
    if (in == null) {
      return null;
    }
    return new MismatchedNormsProducer(in, shuffled, super.getFieldInfos());
  }

  private static class MismatchedNormsProducer extends NormsProducer {

    private final NormsProducer in;
    private final FieldInfos shuffled;
    private final FieldInfos orig;

    MismatchedNormsProducer(NormsProducer in, FieldInfos shuffled, FieldInfos orig) {
      this.in = Objects.requireNonNull(in);
      this.shuffled = shuffled;
      this.orig = orig;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    private FieldInfo remapFieldInfo(FieldInfo field) {
      FieldInfo fi = shuffled.fieldInfo(field.name);
      assert fi != null && fi.number == field.number;
      return orig.fieldInfo(field.name);
    }

    @Override
    public NumericDocValues getNorms(FieldInfo field) throws IOException {
      return in.getNorms(remapFieldInfo(field));
    }

    @Override
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }
  }
}
