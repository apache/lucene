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
package org.apache.lucene.codecs;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValuesReader;

/**
 * Abstract API to write points
 *
 * @lucene.experimental
 */
public abstract class PointsWriter implements Closeable {
  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected PointsWriter() {}

  /** Write all values contained in the provided reader */
  public abstract void writeField(FieldInfo fieldInfo, PointValuesReader values) throws IOException;

  /**
   * Default naive merge implementation for one field: it just re-indexes all the values from the
   * incoming segment. The default codec overrides this for 1D fields and uses a faster but more
   * complex implementation.
   */
  protected void mergeOneField(MergeState mergeState, FieldInfo fieldInfo) throws IOException {
    long maxPointCount = 0;
    for (int i = 0; i < mergeState.pointsReaders.length; i++) {
      PointsReader pointsReader = mergeState.pointsReaders[i];
      if (pointsReader != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getPointDimensionCount() > 0) {
          PointValues values = pointsReader.getValues(fieldInfo.name);
          if (values != null) {
            maxPointCount += values.size();
          }
        }
      }
    }
    final long finalMaxPointCount = maxPointCount;
    writeField(
        fieldInfo,
        new PointValuesReader() {
          @Override
          public void visitDocValues(DocValueVisitor mergedVisitor) throws IOException {
            for (int i = 0; i < mergeState.pointsReaders.length; i++) {
              PointsReader pointsReader = mergeState.pointsReaders[i];
              if (pointsReader == null) {
                // This segment has no points
                continue;
              }
              FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
              if (readerFieldInfo == null) {
                // This segment never saw this field
                continue;
              }

              if (readerFieldInfo.getPointDimensionCount() == 0) {
                // This segment saw this field, but the field did not index points in it:
                continue;
              }

              PointValues values = pointsReader.getValues(fieldInfo.name);
              if (values == null) {
                continue;
              }
              MergeState.DocMap docMap = mergeState.docMaps[i];
              values.visitDocValues(
                  (docID, packedValue) -> {
                    int newDocID = docMap.get(docID);
                    if (newDocID != -1) {
                      // Not deleted:
                      mergedVisitor.visit(newDocID, packedValue);
                    }
                  });
            }
          }

          @Override
          public long size() {
            return finalMaxPointCount;
          }
        });
  }

  /**
   * Default merge implementation to merge incoming points readers by visiting all their points and
   * adding to this writer
   */
  public void merge(MergeState mergeState) throws IOException {
    // check each incoming reader
    for (PointsReader reader : mergeState.pointsReaders) {
      if (reader != null) {
        reader.checkIntegrity();
      }
    }
    // merge field at a time
    for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
      if (fieldInfo.getPointDimensionCount() != 0) {
        mergeOneField(mergeState, fieldInfo);
      }
    }
    finish();
  }

  /** Called once at the end before close */
  public abstract void finish() throws IOException;
}
