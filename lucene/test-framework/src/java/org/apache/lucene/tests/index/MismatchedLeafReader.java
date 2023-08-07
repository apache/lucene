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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;

/**
 * Shuffles field numbers around to try to trip bugs where field numbers are assumed to always be
 * consistent across segments.
 */
public class MismatchedLeafReader extends FilterLeafReader {
  final FieldInfos shuffled;

  /** Creates a new reader which will renumber fields in {@code in} */
  public MismatchedLeafReader(LeafReader in, Random random) {
    super(in);
    shuffled = shuffleInfos(in.getFieldInfos(), random);
  }

  @Override
  public FieldInfos getFieldInfos() {
    return shuffled;
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    in.document(docID, new MismatchedVisitor(visitor));
  }

  @Override
  public StoredFields storedFields() throws IOException {
    final StoredFields inStoredFields = in.storedFields();
    return new StoredFields() {
      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        inStoredFields.document(docID, new MismatchedVisitor(visitor));
      }
    };
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
  public void searchNearestVectors(
      String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void searchNearestVectors(
      String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
    in.searchNearestVectors(field, target, knnCollector, acceptDocs);
  }

  static FieldInfos shuffleInfos(FieldInfos infos, Random random) {
    // first, shuffle the order
    List<FieldInfo> shuffled = new ArrayList<>();
    for (FieldInfo info : infos) {
      shuffled.add(info);
    }
    Collections.shuffle(shuffled, random);

    // now renumber:
    for (int i = 0; i < shuffled.size(); i++) {
      FieldInfo oldInfo = shuffled.get(i);
      // TODO: should we introduce "gaps" too?
      FieldInfo newInfo =
          new FieldInfo(
              oldInfo.name, // name
              i, // number
              oldInfo.hasVectors(), // storeTermVector
              oldInfo.omitsNorms(), // omitNorms
              oldInfo.hasPayloads(), // storePayloads
              oldInfo.getIndexOptions(), // indexOptions
              oldInfo.getDocValuesType(), // docValuesType
              oldInfo.getDocValuesGen(), // dvGen
              oldInfo.attributes(), // attributes
              oldInfo.getPointDimensionCount(), // data dimension count
              oldInfo.getPointIndexDimensionCount(), // index dimension count
              oldInfo.getPointNumBytes(), // dimension numBytes
              oldInfo.getVectorDimension(), // number of dimensions of the field's vector
              oldInfo.getVectorEncoding(), // numeric type of vector samples
              // distance function for calculating similarity of the field's vector
              oldInfo.getVectorSimilarityFunction(),
              oldInfo.isSoftDeletesField()); // used as soft-deletes field
      shuffled.set(i, newInfo);
    }

    return new FieldInfos(shuffled.toArray(new FieldInfo[shuffled.size()]));
  }

  /** StoredFieldsVisitor that remaps actual field numbers to our new shuffled ones. */
  // TODO: its strange this part of our IR api exposes FieldInfo,
  // no other "user-accessible" codec apis do this?
  class MismatchedVisitor extends StoredFieldVisitor {
    final StoredFieldVisitor in;

    MismatchedVisitor(StoredFieldVisitor in) {
      this.in = in;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
      in.binaryField(renumber(fieldInfo), value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
      in.stringField(
          renumber(fieldInfo), Objects.requireNonNull(value, "String value should not be null"));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
      in.intField(renumber(fieldInfo), value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
      in.longField(renumber(fieldInfo), value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
      in.floatField(renumber(fieldInfo), value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
      in.doubleField(renumber(fieldInfo), value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return in.needsField(renumber(fieldInfo));
    }

    FieldInfo renumber(FieldInfo original) {
      FieldInfo renumbered = shuffled.fieldInfo(original.name);
      if (renumbered == null) {
        throw new AssertionError("stored fields sending bogus infos!");
      }
      return renumbered;
    }
  }
}
