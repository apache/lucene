/**
 * Sandbox faceting - Collectors that compute facets.
 * Facet Ordinals/Ids: Each doc may have different facets and therefore, different facet ordinals.
 * For e.g. a book can have Author, Publish Date, Page Count etc. as facets. The specific value for each of these Facets
 * for a book can be mapped to an ordinal. Facet Ids may be common across different book documents.
 * FacetCutter: Can interpret Facets of a specific type for a doc type and output all the Facet Ordinals for the
 * type for the doc.
 * Facet Recorders: record data per ordinal. Some recorders may compute aggregations and record per ordinal data
 * aggregated across an index.
 */
package org.apache.lucene.sandbox.facet;