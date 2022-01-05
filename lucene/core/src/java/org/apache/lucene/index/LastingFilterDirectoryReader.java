package org.apache.lucene.index;

import java.io.IOException;

public abstract class LastingFilterDirectoryReader extends FilterDirectoryReader {
    protected final CacheHelper readerCacheHelper;

    /**
     * Create a new FilterDirectoryReader that filters a passed in DirectoryReader, using the supplied
     * SubReaderWrapper to wrap its subreader.
     *
     * @param in      the DirectoryReader to filter
     * @param wrapper the SubReaderWrapper to use to wrap subreaders
     */
    public LastingFilterDirectoryReader(DirectoryReader in, SubReaderWrapper wrapper) throws IOException {
        super(in, wrapper);
        this.readerCacheHelper = DelegatingCacheHelper.from(in.getReaderCacheHelper());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }
}
