/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.knn.common.KNNConstants;

import java.io.IOException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.Function;

@Log4j2
public class JVectorFormat extends KnnVectorsFormat {
    public static final String NAME = "JVectorFormat";
    public static final String META_CODEC_NAME = "JVectorVectorsFormatMeta";
    public static final String VECTOR_INDEX_CODEC_NAME = "JVectorVectorsFormatIndex";
    public static final String NEIGHBORS_SCORE_CACHE_CODEC_NAME = "JVectorVectorsFormatNeighborsScoreCache";
    public static final String JVECTOR_FILES_SUFFIX = "jvector";
    public static final String META_EXTENSION = "meta-" + JVECTOR_FILES_SUFFIX;
    public static final String VECTOR_INDEX_EXTENSION = "data-" + JVECTOR_FILES_SUFFIX;
    public static final String NEIGHBORS_SCORE_CACHE_EXTENSION = "neighbors-score-cache-" + JVECTOR_FILES_SUFFIX;

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;
    public static final int DEFAULT_MAX_CONN = 32;
    public static final int DEFAULT_BEAM_WIDTH = 100;
    // Unfortunately, this can't be managed yet by the OpenSearch ThreadPool because it's not supporting {@link ForkJoinPool} types
    public static final ForkJoinPool SIMD_POOL_MERGE = getPhysicalCoreExecutor();
    public static final ForkJoinPool SIMD_POOL_FLUSH = getPhysicalCoreExecutor();

    private final int maxConn;
    private final int beamWidth;
    private final Function<Integer, Integer> numberOfSubspacesPerVectorSupplier; // as a function of the original dimension
    private final int minBatchSizeForQuantization;
    private final float alpha;
    private final float neighborOverflow;
    private final boolean hierarchyEnabled;

    public JVectorFormat() {
        this(
            NAME,
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            KNNConstants.DEFAULT_NEIGHBOR_OVERFLOW_VALUE.floatValue(),
            KNNConstants.DEFAULT_ALPHA_VALUE.floatValue(),
            JVectorFormat::getDefaultNumberOfSubspacesPerVector,
            KNNConstants.DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION,
            KNNConstants.DEFAULT_HIERARCHY_ENABLED
        );
    }

    public JVectorFormat(int minBatchSizeForQuantization) {
        this(
            NAME,
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            KNNConstants.DEFAULT_NEIGHBOR_OVERFLOW_VALUE.floatValue(),
            KNNConstants.DEFAULT_ALPHA_VALUE.floatValue(),
            JVectorFormat::getDefaultNumberOfSubspacesPerVector,
            minBatchSizeForQuantization,
            KNNConstants.DEFAULT_HIERARCHY_ENABLED
        );
    }

    public JVectorFormat(
        int maxConn,
        int beamWidth,
        float neighborOverflow,
        float alpha,
        Function<Integer, Integer> numberOfSubspacesPerVectorSupplier,
        int minBatchSizeForQuantization,
        boolean hierarchyEnabled
    ) {
        this(
            NAME,
            maxConn,
            beamWidth,
            neighborOverflow,
            alpha,
            numberOfSubspacesPerVectorSupplier,
            minBatchSizeForQuantization,
            hierarchyEnabled
        );
    }

    public JVectorFormat(
        String name,
        int maxConn,
        int beamWidth,
        float neighborOverflow,
        float alpha,
        Function<Integer, Integer> numberOfSubspacesPerVectorSupplier,
        int minBatchSizeForQuantization,
        boolean hierarchyEnabled
    ) {
        super(name);
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
        this.numberOfSubspacesPerVectorSupplier = numberOfSubspacesPerVectorSupplier;
        this.minBatchSizeForQuantization = minBatchSizeForQuantization;
        this.alpha = alpha;
        this.neighborOverflow = neighborOverflow;
        this.hierarchyEnabled = hierarchyEnabled;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new JVectorWriter(
            state,
            maxConn,
            beamWidth,
            neighborOverflow,
            alpha,
            numberOfSubspacesPerVectorSupplier,
            minBatchSizeForQuantization,
            hierarchyEnabled
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new JVectorReader(state);
    }

    @Override
    public int getMaxDimensions(String s) {
        // Not a hard limit, but a reasonable default
        return 8192;
    }

    /**
     * This method returns the default number of subspaces per vector for a given original dimension.
     * Should be used as a default value for the number of subspaces per vector in case no value is provided.
     *
     * @param originalDimension original vector dimension
     * @return default number of subspaces per vector
     */
    public static int getDefaultNumberOfSubspacesPerVector(int originalDimension) {
        // the idea here is that higher dimensions compress well, but not so well that we should use fewer bits
        // than a lower-dimension vector, which is what you could get with cutoff points to switch between (e.g.)
        // D*0.5 and D*0.25. Thus, the following ensures that bytes per vector is strictly increasing with D.
        int compressedBytes;
        if (originalDimension <= 32) {
            // We are compressing from 4-byte floats to single-byte codebook indexes,
            // so this represents compression of 4x
            // * GloVe-25 needs 25 BPV to achieve good recall
            compressedBytes = originalDimension;
        } else if (originalDimension <= 64) {
            // * GloVe-50 performs fine at 25
            compressedBytes = 32;
        } else if (originalDimension <= 200) {
            // * GloVe-100 and -200 perform well at 50 and 100 BPV, respectively
            compressedBytes = (int) (originalDimension * 0.5);
        } else if (originalDimension <= 400) {
            // * NYTimes-256 actually performs fine at 64 BPV but we'll be conservative
            // since we don't want BPV to decrease
            compressedBytes = 100;
        } else if (originalDimension <= 768) {
            // allow BPV to increase linearly up to 192
            compressedBytes = (int) (originalDimension * 0.25);
        } else if (originalDimension <= 1536) {
            // * ada002 vectors have good recall even at 192 BPV = compression of 32x
            compressedBytes = 192;
        } else {
            // We have not tested recall with larger vectors than this, let's let it increase linearly
            compressedBytes = (int) (originalDimension * 0.125);
        }
        return compressedBytes;
    }

    public static ForkJoinPool getPhysicalCoreExecutor() {
        final int estimatedPhysicalCoreCount = Integer.getInteger(
            "jvector.physical_core_count",
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2)
        );
        assert estimatedPhysicalCoreCount > 0 && estimatedPhysicalCoreCount <= Runtime.getRuntime().availableProcessors()
            : "Invalid core count: " + estimatedPhysicalCoreCount;
        final ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
            ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            thread.setPriority(Thread.NORM_PRIORITY - 2);
            return thread;
        };

        log.info("Creating SIMD ForkJoinPool with {} physical cores for JVector SIMD operations", estimatedPhysicalCoreCount);
        return new ForkJoinPool(estimatedPhysicalCoreCount, factory, null, true);
    }
}
