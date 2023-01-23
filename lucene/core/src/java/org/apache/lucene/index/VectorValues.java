package org.apache.lucene.index;

import org.apache.lucene.document.KnnFloatVectorField;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * KnnFloatVectorField}.
 *
 * @deprecated use {@link FloatVectorValues} instead
 */
@Deprecated
public abstract class VectorValues extends FloatVectorValues {
}
