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

import java.io.IOException;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedSPILoader;

/**
 * Encodes/decodes per-document vector and any associated indexing structures required to support
 * nearest-neighbor search
 */
public abstract class KnnVectorsFormat implements NamedSPILoader.NamedSPI {

  /** The maximum number of vector dimensions */
  public static final int DEFAULT_MAX_DIMENSIONS = 1024;

  /**
   * This static holder class prevents classloading deadlock by delaying init of doc values formats
   * until needed.
   */
  private static final class Holder {
    private static final NamedSPILoader<KnnVectorsFormat> LOADER =
        new NamedSPILoader<>(KnnVectorsFormat.class);

    private Holder() {}

    static NamedSPILoader<KnnVectorsFormat> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a KnnVectorsFormat name before all formats could be initialized. "
                + "This likely happens if you call KnnVectorsFormat#forName from a KnnVectorsFormat's ctor.");
      }
      return LOADER;
    }
  }

  private final String name;

  /** Sole constructor */
  protected KnnVectorsFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /** looks up a format by name */
  public static KnnVectorsFormat forName(String name) {
    return Holder.getLoader().lookup(name);
  }

  /** Returns a {@link KnnVectorsWriter} to write the vectors to the index. */
  public abstract KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException;

  /** Returns a {@link KnnVectorsReader} to read the vectors from the index. */
  public abstract KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException;

  /**
   * Returns the maximum number of vector dimensions supported by this codec for the given field
   * name
   *
   * <p>Codecs should override this method to specify the maximum number of dimensions they support.
   *
   * @param fieldName the field name
   * @return the maximum number of vector dimensions.
   */
  public int getMaxDimensions(String fieldName) {
    return DEFAULT_MAX_DIMENSIONS;
  }

  /**
   * EMPTY throws an exception when written. It acts as a sentinel indicating a Codec that does not
   * support vectors.
   */
  public static final KnnVectorsFormat EMPTY =
      new KnnVectorsFormat("EMPTY") {
        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) {
          throw new UnsupportedOperationException("Attempt to write EMPTY vector values");
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) {
          return new KnnVectorsReader() {
            @Override
            public void checkIntegrity() {}

            @Override
            public FloatVectorValues getFloatVectorValues(String field) {
              throw new UnsupportedOperationException();
            }

            @Override
            public ByteVectorValues getByteVectorValues(String field) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void search(
                String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void search(
                String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void close() {}

            @Override
            public long ramBytesUsed() {
              return 0;
            }
          };
        }
      };
}
