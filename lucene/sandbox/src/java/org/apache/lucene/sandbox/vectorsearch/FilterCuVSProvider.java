package org.apache.lucene.sandbox.vectorsearch;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.HnswIndex;
import com.nvidia.cuvs.spi.CuVSProvider;

import java.nio.file.Path;

public class FilterCuVSProvider implements CuVSProvider {

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
    public BruteForceIndex.Builder newBruteForceIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newBruteForceIndexBuilder(cuVSResources);
    }

    @Override
    public CagraIndex.Builder newCagraIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newCagraIndexBuilder(cuVSResources);
    }

    @Override
    public HnswIndex.Builder newHnswIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newHnswIndexBuilder(cuVSResources);
    }
}
