package org.apache.lucene.facet.taxonomy;

import org.apache.lucene.util.BytesRef;

public class IntPerLabelAssociationFacetField extends PerLabelAssociationFacetField {
    public IntPerLabelAssociationFacetField(int assoc, String dim, String... path) {
        super(IntAssociationFacetField.intToBytesRef(assoc), dim, path);
    }
}
