package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.LengthOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;

import java.io.IOException;

/**
 * Base implementation for {@link FacetBuilder}, provides common functionality
 * that can be shared between different facet types.
 */
abstract class BaseFacetBuilder<C extends BaseFacetBuilder<C>> extends FacetBuilder {
    final String dimension;
    final String[] path;
    CountFacetRecorder countRecorder;
    private FacetFieldCollectorManager<CountFacetRecorder> collectorManager;
    private int topN = -1;

    BaseFacetBuilder(String dimension, String... path) {
        this.dimension = dimension;
        this.path = path;
    }

    public final C withTopN(int n) {
        this.topN = n;
        return self();
    }


    /** Create {@link FacetCutter} */
    abstract FacetCutter createFacetCutter();

    OrdinalIterator getMatchingOrdinalIterator() throws IOException {
        return countRecorder.recordedOrds();
    }

    final Number getValue(int facetOrd) {
        // TODO: support other aggregations
        return countRecorder.getCount(facetOrd);
    }

    abstract Number getOverallValue() throws IOException;

    abstract OrdToLabel ordToLabel();

    @Override
    final FacetBuilder initOrReuseCollector(FacetBuilder similar) {
        // share recorders between FacetBuilders that share CollectorManager
        // TODO: add support for other aggregation types, e.g. float/int associations
        //       and long aggregations
        if (similar instanceof BaseFacetBuilder<?> castedSimilar) {
            this.countRecorder = castedSimilar.countRecorder;
            return similar;
        } else {
            this.countRecorder = new CountFacetRecorder();
            this.collectorManager = new FacetFieldCollectorManager<>(createFacetCutter(), countRecorder);
            return this;
        }
    }

    @Override
    final FacetFieldCollectorManager<CountFacetRecorder> getCollectorManager() {
        return this.collectorManager;
    }

    @Override
    public final FacetResult getResult() {
        assert countRecorder != null : "must not be called before collect";
        ComparableSupplier<?> comparableSupplier = ComparableUtils.byCount(countRecorder);
        OrdinalIterator ordinalIterator;
        try {
            LengthOrdinalIterator lengthOrdinalIterator = new LengthOrdinalIterator(getMatchingOrdinalIterator());
            ordinalIterator = lengthOrdinalIterator;

            if (topN != -1) {
                ordinalIterator =
                        new TopnOrdinalIterator<>(ordinalIterator, comparableSupplier, topN);
            }
            // TODO: else sort?

            // Build results
            OrdToLabel ordToLabel = ordToLabel();

            int[] ordinalsArray = ordinalIterator.toArray();
            FacetLabel[] labels = ordToLabel.getLabels(ordinalsArray);

            LabelAndValue[] labelsAndValues = new LabelAndValue[labels.length];
            for (int i = 0; i < ordinalsArray.length; i++) {
                labelsAndValues[i] = new LabelAndValue(labels[i].lastComponent(), getValue(ordinalsArray[i]), countRecorder.getCount(ordinalsArray[i]));
            }
            return new FacetResult(
                    dimension,
                    path,
                    getOverallValue(),
                    labelsAndValues,
                    lengthOrdinalIterator.length());


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Util method to be able to extend this class. */
    abstract C self();
}
