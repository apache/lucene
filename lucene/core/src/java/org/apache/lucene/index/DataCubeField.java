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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Stores information about dimensions and metrics associated with DataCubeField Further can be
 * customized in DataCubeFieldProvider
 */
public class DataCubeField {

  private final String name;
  private final Set<String> dimensionFields;
  private final List<MetricField> metricFields;

  /** Sole constructor */
  public DataCubeField(String name, Set<String> dimensionFields, List<MetricField> metricFields) {
    this.name = name;
    this.dimensionFields = dimensionFields;
    this.metricFields = metricFields;
  }

  /** Returns set of dimensions associated with this DataCubeField */
  public Set<String> getDimensionFields() {
    return dimensionFields;
  }

  /** Returns set of metrics associated with this DataCubeField */
  public List<MetricField> getMetricFields() {
    return metricFields;
  }

  /** Returns name of this DataCubeField */
  public String getName() {
    return name;
  }

  /** Returns name of this DataCubeFieldProvider */
  public String getProviderName() {
    return Provider.NAME;
  }

  /** Metric field which encapsulates name and associated aggregation function */
  public static class MetricField {
    private final String name;
    // TODO : if we make this enum, how to enable it for extensions
    // in custom formats - so keeping it as string for now
    private final String metricFunction;

    /** sole constructor */
    public MetricField(String name, String metricFunction) {
      this.name = name;
      this.metricFunction = metricFunction;
    }
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
      String name = in.readString();
      Set<String> dimensionFields = in.readSetOfStrings();
      List<MetricField> metricFields = new ArrayList<>();
      int metricFieldsCount = in.readVInt();
      for (int i = 0; i < metricFieldsCount; i++) {
        metricFields.add(new MetricField(in.readString(), in.readString()));
      }
      return new DataCubeField(name, dimensionFields, metricFields);
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
    out.writeSetOfStrings(dimensionFields);
    out.writeVInt(metricFields.size());
    for (MetricField metricfield : metricFields) {
      out.writeString(metricfield.name);
      out.writeString(metricfield.metricFunction);
    }
  }
}
