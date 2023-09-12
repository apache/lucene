package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.util.BytesRef;

import java.util.HashMap;
import java.util.Map;

public abstract class PerLabelAssociationFacetField extends AssociationFacetField {
    // This global map ensures we record a single association per facet label.
    private static Map<FacetLabel, BytesRef> valuesPerLabel = new HashMap<>();

    public PerLabelAssociationFacetField(BytesRef assoc, String dim, String... path) {
        super(valuesPerLabel.merge(new FacetLabel(dim, path), assoc, (oldV, newV) -> oldV), dim, path);
    }
}
