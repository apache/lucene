package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.spi.CuVSProvider;
import com.nvidia.cuvs.spi.CuVSServiceProvider;

public class FilterCuVSServiceProvider extends CuVSServiceProvider {
  @Override
  public CuVSProvider get(CuVSProvider builtinProvider) {
    return new FilterCuVSProvider(builtinProvider);
  }
}
