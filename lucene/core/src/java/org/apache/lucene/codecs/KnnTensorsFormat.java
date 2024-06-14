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

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedSPILoader;

import java.io.IOException;

/**
 * Encodes/decodes per-document tensors and any associated indexing structures required to support
 * nearest-neighbor search
 */
public abstract class KnnTensorsFormat implements NamedSPILoader.NamedSPI {

  /** The maximum number of dimensions supported for each vector in the tensor */
  public static final int DEFAULT_MAX_DIMENSIONS = 1024;

  /** The maximum rank supported for each tensor */
  public static final int DEFAULT_MAX_RANK = 2;

  /**
   * This static holder class prevents classloading deadlock by delaying init of doc values formats
   * until needed.
   */
  private static final class Holder {
    private static final NamedSPILoader<KnnTensorsFormat> LOADER =
        new NamedSPILoader<>(KnnTensorsFormat.class);

    private Holder() {}

    static NamedSPILoader<KnnTensorsFormat> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a KnnTensorsFormat name before all formats could be initialized. "
                + "This likely happens if you call KnnTensorsFormat#forName from a KnnTensorsFormat's ctor.");
      }
      return LOADER;
    }
  }

  private final String name;

  /** Sole constructor */
  protected KnnTensorsFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /** looks up a format by name */
  public static KnnTensorsFormat forName(String name) {
    return Holder.getLoader().lookup(name);
  }

  /** Returns a {@link KnnTensorsWriter} to write the tensors to the index. */
  public abstract KnnTensorsWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /** Returns a {@link KnnTensorsReader} to read the tensors from the index. */
  public abstract KnnTensorsReader fieldsReader(SegmentReadState state) throws IOException;

  /**
   * Returns the maximum number of dimensions per vector supported by this codec
   * for the given field name
   *
   * <p>Codecs implement this method to specify the maximum number of dimensions they support.
   *
   * @param fieldName the field name
   * @return the maximum number of vector dimensions.
   */
  public abstract int getMaxDimensions(String fieldName);

  /**
   * EMPTY throws an exception when written. It acts as a sentinel indicating a Codec that does not
   * support vectors.
   */
  public static final KnnTensorsFormat EMPTY =
      new KnnTensorsFormat("EMPTY") {
        @Override
        public KnnTensorsWriter fieldsWriter(SegmentWriteState state) {
          throw new UnsupportedOperationException("Attempt to write EMPTY vector values");
        }

        @Override
        public KnnTensorsReader fieldsReader(SegmentReadState state) {
          return new KnnTensorsReader() {
//            @Override
//            public void checkIntegrity() {}
//
//            @Override
//            public FloatVectorValues getFloatVectorValues(String field) {
//              throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public ByteVectorValues getByteVectorValues(String field) {
//              throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public void search(
//                String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {
//              throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public void search(
//                String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {
//              throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public void close() {}
          };
        }

        @Override
        public int getMaxDimensions(String fieldName) {
          return 0;
        }
      };
}
