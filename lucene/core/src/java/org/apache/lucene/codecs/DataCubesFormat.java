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
import org.apache.lucene.index.DataCubesConfig;
import org.apache.lucene.index.DataCubesDocValuesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.NamedSPILoader;

/** Encodes/decodes indexing structures required to support data cubes */
public abstract class DataCubesFormat implements NamedSPILoader.NamedSPI {

  private final String name;

  /** Sole constructor. */
  protected DataCubesFormat(String name) {
    NamedSPILoader.checkServiceName(name);
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /** Returns a {@link DataCubesProducer} to read dataCubesValues from the index. */
  public abstract DataCubesProducer<?> fieldsProducer(SegmentReadState state) throws IOException;

  /**
   * Returns a {@link org.apache.lucene.index.DataCubesDocValuesConsumer} to write dataCubesValues
   * to the index based on docValues NOTE: by the time this call returns, it must hold open any
   * files it will need to use; else, those files may be deleted. Additionally, required files may
   * be deleted during the execution of this call before there is a chance to open them. Under these
   * circumstances an IOException should be thrown by the implementation. IOExceptions are expected
   * and will automatically cause a retry of the segment opening logic with the newly revised
   * segments
   */
  public abstract DataCubesDocValuesConsumer fieldsConsumer(
      SegmentWriteState state, DataCubesConfig compositeConfig) throws IOException;

  /** A {@code DataCubesFormat} that has nothing indexed */
  public static final DataCubesFormat EMPTY =
      new DataCubesFormat("EMPTY") {
        @Override
        public DataCubesProducer<?> fieldsProducer(SegmentReadState state) throws IOException {
          return null;
        }

        @Override
        public DataCubesDocValuesConsumer fieldsConsumer(
            SegmentWriteState state, DataCubesConfig compositeConfig) throws IOException {
          throw new UnsupportedOperationException("Attempt to write EMPTY DataCube values");
        }
      };
}
