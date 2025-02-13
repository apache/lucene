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
package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.HnswIndex;
import com.nvidia.cuvs.spi.CuVSProvider;
import java.nio.file.Path;

/*package-private*/ class FilterCuVSProvider implements CuVSProvider {

  private final CuVSProvider delegate;

  FilterCuVSProvider(CuVSProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public Path nativeLibraryPath() {
    return CuVSProvider.TMPDIR;
  }

  @Override
  public CuVSResources newCuVSResources(Path tempPath) throws Throwable {
    return delegate.newCuVSResources(tempPath);
  }

  @Override
  public BruteForceIndex.Builder newBruteForceIndexBuilder(CuVSResources cuVSResources)
      throws UnsupportedOperationException {
    return delegate.newBruteForceIndexBuilder(cuVSResources);
  }

  @Override
  public CagraIndex.Builder newCagraIndexBuilder(CuVSResources cuVSResources)
      throws UnsupportedOperationException {
    return delegate.newCagraIndexBuilder(cuVSResources);
  }

  @Override
  public HnswIndex.Builder newHnswIndexBuilder(CuVSResources cuVSResources)
      throws UnsupportedOperationException {
    return delegate.newHnswIndexBuilder(cuVSResources);
  }
}
