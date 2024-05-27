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
import org.apache.lucene.util.NamedSPILoader;

/**
 * Reads/Writes a named DataCubeField from a segment info file, used to record index DataCubeField
 */
public abstract class DataCubeFieldProvider implements NamedSPILoader.NamedSPI {

  /** Name of the DataCubeFieldProvider it is registered upon */
  protected final String name;

  private static class Holder {
    private static final NamedSPILoader<DataCubeFieldProvider> LOADER =
        new NamedSPILoader<>(DataCubeFieldProvider.class);

    static NamedSPILoader<DataCubeFieldProvider> getLoader() {
      if (LOADER == null) {
        throw new IllegalStateException(
            "You tried to lookup a DataCubeFieldProvider by name before all DataCubeFieldProviders could be initialized. "
                + "This likely happens if you call DataCubeFieldProvider#forName from a DataCubeFieldProvider's ctor.");
      }
      return LOADER;
    }
  }

  /** Get provider for the DataCubeField */
  public static DataCubeFieldProvider getProvider(DataCubeField cf) {
    return DataCubeFieldProvider.forName(cf.getName());
  }

  /** Looks up a DataCubeFieldProvider by name */
  public static DataCubeFieldProvider forName(String name) {
    return DataCubeFieldProvider.Holder.getLoader().lookup(name);
  }

  /** Lists all available DataCubeFieldProviders */
  public static Set<String> availableDataCubeFieldProviders() {
    return DataCubeFieldProvider.Holder.getLoader().availableServices();
  }

  /**
   * Creates a new DataCubeFieldProvider.
   *
   * <p>The provided name will be written into the index segment: in order to for the segment to be
   * read this class should be registered with Java's SPI mechanism (registered in META-INF/ of your
   * jar file, etc).
   *
   * @param name must be all ascii alphanumeric, and less than 128 characters in length.
   */
  protected DataCubeFieldProvider(String name) {
    this.name = name;
  }

  /** Writes a DataCubeField to a DataOutput */
  public static void write(DataCubeField cf, DataOutput output) throws IOException {
    DataCubeFieldProvider provider = DataCubeFieldProvider.forName(cf.getProviderName());
    provider.writeDataCubeField(cf, output);
  }

  @Override
  public String getName() {
    return name;
  }

  /** Reads a DataCubeField from serialized bytes */
  public abstract DataCubeField readDataCubeField(DataInput in) throws IOException;

  /**
   * Writes a DataCubeField to a DataOutput
   *
   * <p>This is used to record DataCubeField information in segment headers
   */
  public abstract void writeDataCubeField(DataCubeField cf, DataOutput out) throws IOException;
}
