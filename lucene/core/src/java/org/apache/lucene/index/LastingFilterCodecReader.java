package org.apache.lucene.index;

public abstract class LastingFilterCodecReader extends FilterCodecReader {
    private final CacheHelper readerCacheHelper;

    /**
     * Creates a new FilterCodecReader.
     *
     * @param in the underlying CodecReader instance.
     */
    public LastingFilterCodecReader(CodecReader in) {
        super(in);
        this.readerCacheHelper = DelegatingCacheHelper.from(in.getReaderCacheHelper());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return readerCacheHelper;
    }
}
