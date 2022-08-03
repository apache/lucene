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
package org.apache.lucene.codecs.monitoring;

import java.io.IOException;
import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * A codec track the merge time of each part of index and vent it to {@link TimeMetric} It forwards
 * all its method calls to another codec.
 */
public class MergeTimeTrackingCodec extends FilterCodec {
  private final TimeMetric metric;

  public MergeTimeTrackingCodec(Codec codec, TimeMetric metric) {
    super(codec.getName(), codec);
    this.metric = metric;
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    DocValuesFormat docValuesFormat = delegate.docValuesFormat();
    return new FilterDocValuesFormat(docValuesFormat) {
      @Override
      public DocValuesConsumer fieldsConsumer(SegmentWriteState var1) throws IOException {
        return new FilterDocValuesConsumer(in.fieldsConsumer(var1)) {
          @Override
          public void merge(MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            in.merge(mergeState);
            metric.addNano("DocValuesMergeTime", System.nanoTime() - t0);
          }
        };
      }
    };
  }

  @Override
  public PointsFormat pointsFormat() {
    PointsFormat pointsFormat = delegate.pointsFormat();
    return new FilterPointsFormat(pointsFormat) {
      @Override
      public PointsWriter fieldsWriter(SegmentWriteState var1) throws IOException {
        return new FilterPointsWriter(in.fieldsWriter(var1)) {
          @Override
          public void merge(MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            in.merge(mergeState);
            metric.addNano("PointsMergeTime", System.nanoTime() - t0);
          }
        };
      }
    };
  }

  @Override
  public NormsFormat normsFormat() {
    NormsFormat normsFormat = delegate.normsFormat();
    return new FilterNormsFormat(normsFormat) {
      @Override
      public NormsConsumer normsConsumer(SegmentWriteState var1) throws IOException {
        return new FilterNormsConsumer(in.normsConsumer(var1)) {
          @Override
          public void merge(MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            in.merge(mergeState);
            metric.addNano("NormsMergeTime", System.nanoTime() - t0);
          }
        };
      }
    };
  }

  @Override
  public PostingsFormat postingsFormat() {
    PostingsFormat postingsFormat = delegate.postingsFormat();

    return new FilterPostingsFormat(postingsFormat) {
      @Override
      public FieldsConsumer fieldsConsumer(SegmentWriteState var1) throws IOException {
        return new FilterFieldsConsumer(in.fieldsConsumer(var1)) {
          @Override
          public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
            long t0 = System.nanoTime();
            in.merge(mergeState, norms);
            metric.addNano("PostingsMergeTime", System.nanoTime() - t0);
          }
        };
      }
    };
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    StoredFieldsFormat storedFieldsFormat = delegate.storedFieldsFormat();
    return new FilterStoredFieldsFormat(storedFieldsFormat) {
      @Override
      public StoredFieldsWriter fieldsWriter(
          Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        return new FilterStoredFieldsWriter(in.fieldsWriter(directory, segmentInfo, ioContext)) {
          @Override
          public int merge(MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            int docCount = in.merge(mergeState);
            metric.addNano("StoredFieldsMergeTime", System.nanoTime() - t0);
            return docCount;
          }
        };
      }
    };
  }

  @Override
  public final TermVectorsFormat termVectorsFormat() {
    TermVectorsFormat termVectorsFormat = delegate.termVectorsFormat();
    return new FilterTermVectorsFormat(termVectorsFormat) {
      @Override
      public TermVectorsWriter vectorsWriter(
          Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        TermVectorsWriter termVectorsWriter = in.vectorsWriter(directory, segmentInfo, ioContext);
        return new FilterTermVectorsWriter(termVectorsWriter) {
          @Override
          public int merge(MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            int docCount = in.merge(mergeState);
            metric.addNano("TermVectorsMergeTime", System.nanoTime() - t0);
            return docCount;
          }
        };
      }
    };
  }

  @Override
  public final KnnVectorsFormat knnVectorsFormat() {
    KnnVectorsFormat knnVectorsFormat = delegate.knnVectorsFormat();
    return new FilterKnnVectorsFormat(knnVectorsFormat) {
      @Override
      public KnnVectorsWriter fieldsWriter(SegmentWriteState var1) throws IOException {
        return new FilterKnnVectorsWriter(in.fieldsWriter(var1)) {
          @Override
          public void merge(final MergeState mergeState) throws IOException {
            long t0 = System.nanoTime();
            in.merge(mergeState);
            metric.addNano("KnnVectorsMergeTime", System.nanoTime() - t0);
          }
        };
      }
    };
  }
}
