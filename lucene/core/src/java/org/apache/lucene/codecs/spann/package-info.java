/**
 * SPANN: Disk-Resident HNSW-IVF Vectors Format.
 *
 * <p>This package contains the implementation of the SPANN vector format, which implements a
 * "Disk-Resident HNSW-IVF" architecture:
 *
 * <ul>
 *   <li><b>Navigation (Tier 1)</b>: Centroids are indexed using {@link
 *       org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat}. This allows efficient
 *       off-heap navigation of millions of centroids.
 *   <li><b>Storage (Tier 2)</b>: Data vectors are stored in a clustered, sequential format on disk,
 *       optimized for large-scale scanning.
 * </ul>
 *
 * <p>The main entry point is {@link org.apache.lucene.codecs.spann.Lucene99SpannVectorsFormat}.
 */
package org.apache.lucene.codecs.spann;
