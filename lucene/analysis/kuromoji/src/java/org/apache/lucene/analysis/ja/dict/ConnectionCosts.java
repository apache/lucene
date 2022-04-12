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
package org.apache.lucene.analysis.ja.dict;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;

/** n-gram connection cost data */
public final class ConnectionCosts extends org.apache.lucene.analysis.morph.ConnectionCosts {

  /**
   * Create a {@link ConnectionCosts} from an external resource path.
   *
   * @param connectionCostsFile where to load connection costs resource
   * @throws IOException if resource was not found or broken
   */
  public ConnectionCosts(Path connectionCostsFile) throws IOException {
    this(() -> Files.newInputStream(connectionCostsFile));
  }

  private ConnectionCosts() throws IOException {
    this(ConnectionCosts::getClassResource);
  }

  private ConnectionCosts(IOSupplier<InputStream> connectionCostResource) throws IOException {
    super(
        connectionCostResource, DictionaryConstants.CONN_COSTS_HEADER, DictionaryConstants.VERSION);
  }

  private static InputStream getClassResource() throws IOException {
    final String resourcePath = ConnectionCosts.class.getSimpleName() + FILENAME_SUFFIX;
    return IOUtils.requireResourceNonNull(
        ConnectionCosts.class.getResourceAsStream(resourcePath), resourcePath);
  }

  public static ConnectionCosts getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private static class SingletonHolder {
    static final ConnectionCosts INSTANCE;

    static {
      try {
        INSTANCE = new ConnectionCosts();
      } catch (IOException ioe) {
        throw new RuntimeException("Cannot load ConnectionCosts.", ioe);
      }
    }
  }
}
