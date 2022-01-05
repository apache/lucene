package org.apache.lucene.index;

public abstract class LastingFilterLeafReader extends FilterLeafReader {
    private final CacheHelper readerCacheHelper;

    /**
     * Construct a FilterLeafReader based on the specified base reader.
     *
     * <p>Note that base reader is closed if this FilterLeafReader is closed.
     *
     * @param in specified base reader.
     */
    protected LastingFilterLeafReader(LeafReader in) {
        super(in);
        this.readerCacheHelper = DelegatingCacheHelper.from(in.getReaderCacheHelper());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }
}
