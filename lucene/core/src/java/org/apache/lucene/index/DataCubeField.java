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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Stores information about dimensions and metrics associated with DataCubeField Further can be
 * customized in DataCubeFieldProvider
 */
public class DataCubeField {

  private final String name;
  private final Set<String> dims;
  private final Set<String> metrics;

  /** Sole constructor */
  public DataCubeField(String name, Set<String> dims, Set<String> metrics) {
    this.name = name;
    this.dims = dims;
    this.metrics = metrics;
  }

  /** Returns set of dimensions associated with this DataCubeField */
  public Set<String> getDims() {
    return dims;
  }

  /** Returns set of metrics associated with this DataCubeField */
  public Set<String> getMetrics() {
    return metrics;
  }

  /** Returns name of this DataCubeField */
  public String getName() {
    return name;
  }

  /** Returns name of this DataCubeFieldProvider */
  public String getProviderName() {
    return Provider.NAME;
  }

  /** Provider for DataCubeField */
  public static final class Provider extends DataCubeFieldProvider {

    /** The name this Provider is registered under */
    public static final String NAME = "DataCubeField";

    /** Creates a new Provider */
    public Provider() {
      super(NAME);
    }

    /** Reads DataCubeField from DataInput */
    @Override
    public DataCubeField readDataCubeField(DataInput in) throws IOException {
      DataCubeField cf =
          new DataCubeField(in.readString(), in.readSetOfStrings(), in.readSetOfStrings());
      return cf;
    }

    /** Writes DataCubeField to DataOutput */
    @Override
    public void writeDataCubeField(DataCubeField cf, DataOutput out) throws IOException {
      cf.serialize(out);
    }
  }

  /** Serializes DataCubeField to DataOutput */
  private void serialize(DataOutput out) throws IOException {
    out.writeString(name);
    out.writeSetOfStrings(dims);
    out.writeSetOfStrings(metrics);
  }
}
