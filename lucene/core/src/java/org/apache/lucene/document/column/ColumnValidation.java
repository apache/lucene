/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.document.column;

import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;

/**
 * Static validation and bounds-checking helpers for the columnar indexing path. These helpers are
 * pure functions over the public column API and {@link IndexableFieldType}; they do not touch any
 * indexing-chain state.
 *
 * @lucene.internal
 */
public final class ColumnValidation {

  private ColumnValidation() {}

  /** Inverted-index feature ({@code indexOptions() != NONE}). */
  static final int FEATURE_INVERSION = 1 << 0;

  /** Stored-fields feature ({@code stored()}). */
  static final int FEATURE_STORED = 1 << 1;

  /** Doc-values feature ({@code docValuesType() != NONE}). */
  static final int FEATURE_DOCVALUES = 1 << 2;

  /** Points feature ({@code pointDimensionCount() != 0}). */
  static final int FEATURE_POINTS = 1 << 3;

  /** KNN-vector feature ({@code vectorDimension() != 0}). */
  static final int FEATURE_VECTOR = 1 << 4;

  /**
   * Returns a bitmask of the indexing features ({@code FEATURE_*}) declared by {@code fieldType}.
   * Used by the column-batch path to enforce that, when several columns share a field name, each
   * feature is carried by at most one column.
   */
  public static int featureMask(IndexableFieldType fieldType) {
    int mask = 0;
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      mask |= FEATURE_INVERSION;
    }
    if (fieldType.stored()) {
      mask |= FEATURE_STORED;
    }
    if (fieldType.docValuesType() != DocValuesType.NONE) {
      mask |= FEATURE_DOCVALUES;
    }
    if (fieldType.pointDimensionCount() != 0) {
      mask |= FEATURE_POINTS;
    }
    if (fieldType.vectorDimension() != 0) {
      mask |= FEATURE_VECTOR;
    }
    return mask;
  }

  /** Returns a human-readable comma-separated list of the feature names set in {@code mask}. */
  public static String featureNames(int mask) {
    StringBuilder sb = new StringBuilder();
    if ((mask & FEATURE_INVERSION) != 0) {
      sb.append("inversion");
    }
    if ((mask & FEATURE_STORED) != 0) {
      if (!sb.isEmpty()) sb.append(", ");
      sb.append("stored");
    }
    if ((mask & FEATURE_DOCVALUES) != 0) {
      if (!sb.isEmpty()) sb.append(", ");
      sb.append("doc values");
    }
    if ((mask & FEATURE_POINTS) != 0) {
      if (!sb.isEmpty()) sb.append(", ");
      sb.append("points");
    }
    if ((mask & FEATURE_VECTOR) != 0) {
      if (!sb.isEmpty()) sb.append(", ");
      sb.append("vectors");
    }
    return "[" + sb + "]";
  }

  /**
   * Throws {@link IllegalArgumentException} if {@code fieldType} declares no indexing feature (no
   * doc values, no points, not stored, no index options, no vectors).
   */
  public static void validateColumnHasIndexingFeature(
      String fieldName, IndexableFieldType fieldType) {
    if (fieldType.docValuesType() == DocValuesType.NONE
        && fieldType.pointDimensionCount() == 0
        && fieldType.stored() == false
        && fieldType.indexOptions() == IndexOptions.NONE
        && fieldType.vectorDimension() == 0) {
      throw new IllegalArgumentException(
          "Column \""
              + fieldName
              + "\" must have a non-NONE docValuesType, point dimensions, be stored,"
              + " have index options, or have vector dimensions");
    }
  }

  /** Validates a {@link LongColumn} against the field type it will feed. */
  public static void validateLongColumn(LongColumn column, IndexableFieldType fieldType) {
    final int pointDims = fieldType.pointDimensionCount();
    if (pointDims != 0) {
      if (pointDims != 1) {
        throw new IllegalArgumentException(
            "LongColumn \""
                + column.name()
                + "\" only supports 1-dimensional point fields, got pointDimensionCount="
                + pointDims);
      }
      final int expectedPointBytes =
          (column.numericKind() == LongColumn.NumericKind.INT
                  || column.numericKind() == LongColumn.NumericKind.FLOAT)
              ? Integer.BYTES
              : Long.BYTES;
      if (fieldType.pointNumBytes() != expectedPointBytes) {
        throw new IllegalArgumentException(
            "LongColumn \""
                + column.name()
                + "\" numericKind="
                + column.numericKind()
                + " requires pointNumBytes="
                + expectedPointBytes
                + ", got "
                + fieldType.pointNumBytes());
      }
    }
    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case INTEGER, LONG, FLOAT, DOUBLE -> {
          // OK.
        }
        case STRING, BINARY ->
            throw new IllegalArgumentException(
                "LongColumn \""
                    + column.name()
                    + "\" storedType="
                    + storedType
                    + " is not supported; use a BinaryColumn for non-numeric stored data");
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "LongColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  /** Validates a {@link BinaryColumn} against the field type it will feed. */
  public static void validateBinaryColumn(BinaryColumn column, IndexableFieldType fieldType) {
    final DocValuesType dvType = fieldType.docValuesType();
    if (dvType == DocValuesType.NUMERIC || dvType == DocValuesType.SORTED_NUMERIC) {
      throw new IllegalArgumentException(
          "BinaryColumn \""
              + column.name()
              + "\" cannot feed docValuesType="
              + dvType
              + "; use a LongColumn");
    }
    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case BINARY, STRING -> {
          // OK.
        }
        case INTEGER, LONG, FLOAT, DOUBLE ->
            throw new IllegalArgumentException(
                "BinaryColumn \""
                    + column.name()
                    + "\" storedType="
                    + storedType
                    + " is not supported; use a LongColumn for numeric stored data");
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "BinaryColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  /** Validates a {@link DictionaryColumn} against the field type it will feed. */
  public static void validateDictionaryColumn(
      DictionaryColumn column, IndexableFieldType fieldType) {
    final DocValuesType dv = fieldType.docValuesType();
    if (dv == DocValuesType.NUMERIC || dv == DocValuesType.SORTED_NUMERIC) {
      throw new IllegalArgumentException(
          "DictionaryColumn \""
              + column.name()
              + "\" cannot feed docValuesType="
              + dv
              + "; use a LongColumn");
    }
    if (dv == DocValuesType.BINARY) {
      throw new IllegalArgumentException(
          "DictionaryColumn \""
              + column.name()
              + "\" cannot feed docValuesType=BINARY (the writer does not dedup terms, so the"
              + " dictionary provides no benefit); use a BinaryColumn");
    }
    if (fieldType.pointDimensionCount() != 0) {
      throw new IllegalArgumentException(
          "DictionaryColumn \""
              + column.name()
              + "\" does not support points (pointDimensionCount must be 0)");
    }
    if (fieldType.stored()) {
      final StoredValue.Type storedType = column.storedType();
      switch (storedType) {
        case BINARY, STRING -> {
          // OK.
        }
        case INTEGER, LONG, FLOAT, DOUBLE ->
            throw new IllegalArgumentException(
                "DictionaryColumn \""
                    + column.name()
                    + "\" storedType="
                    + storedType
                    + " is not supported; use a LongColumn for numeric stored data");
        case DATA_INPUT ->
            throw new IllegalArgumentException(
                "DictionaryColumn \""
                    + column.name()
                    + "\" storedType DATA_INPUT is not supported for columns");
      }
    }
  }

  /** Validates a {@link TokenStreamColumn} against the field type it will feed. */
  public static void validateTokenStreamColumn(
      TokenStreamColumn column, IndexableFieldType fieldType) {
    if (fieldType.indexOptions() == IndexOptions.NONE || fieldType.tokenized() == false) {
      throw new IllegalArgumentException(
          "TokenStreamColumn \""
              + column.name()
              + "\" requires indexOptions != NONE and tokenized == true; got indexOptions="
              + fieldType.indexOptions()
              + ", tokenized="
              + fieldType.tokenized());
    }
    if (fieldType.stored()
        || fieldType.docValuesType() != DocValuesType.NONE
        || fieldType.pointDimensionCount() != 0
        || fieldType.vectorDimension() != 0) {
      throw new IllegalArgumentException(
          "TokenStreamColumn \""
              + column.name()
              + "\" must be inverted-only: stored=false, docValuesType=NONE,"
              + " pointDimensionCount=0, vectorDimension=0");
    }
  }

  /** Validates a {@link VectorColumn} against the field type it will feed. */
  public static void validateVectorColumn(VectorColumn<?> column, IndexableFieldType fieldType) {
    if (fieldType.vectorDimension() <= 0) {
      throw new IllegalArgumentException(
          "VectorColumn \""
              + column.name()
              + "\" requires fieldType.vectorDimension() > 0; got "
              + fieldType.vectorDimension());
    }
    if (fieldType.docValuesType() != DocValuesType.NONE
        || fieldType.pointDimensionCount() != 0
        || fieldType.stored()
        || fieldType.indexOptions() != IndexOptions.NONE) {
      throw new IllegalArgumentException(
          "VectorColumn \""
              + column.name()
              + "\" must be vector-only: docValuesType=NONE, pointDimensionCount=0,"
              + " stored=false, indexOptions=NONE");
    }
  }

  /** Throws if {@code batchDocID} is outside {@code [0, numDocs)}. */
  public static void checkDocID(Column column, int batchDocID, int numDocs) {
    if (batchDocID < 0 || batchDocID >= numDocs) {
      throw new IllegalArgumentException(
          "Column \""
              + column.name()
              + "\" returned batch doc-id "
              + batchDocID
              + " which is out of range [0, "
              + numDocs
              + ")");
    }
  }

  /** Throws if a dense column did not produce exactly {@code numDocs} values. */
  public static void checkDenseCount(Column column, int consumed, int numDocs) {
    if (consumed != numDocs) {
      throw new IllegalArgumentException(
          "Dense column \""
              + column.name()
              + "\" provided "
              + consumed
              + " values but batch has "
              + numDocs
              + " documents");
    }
  }

  /** Throws if a vector cursor doc-id is not strictly greater than the previous one. */
  public static void checkVectorDocIDStrictlyIncreasing(
      VectorColumn<?> column, int batchDocID, int prevBatchDocID) {
    if (batchDocID <= prevBatchDocID) {
      throw new IllegalArgumentException(
          "VectorColumn \""
              + column.name()
              + "\" must yield strictly increasing batch doc-ids; got "
              + batchDocID
              + " after "
              + prevBatchDocID);
    }
  }

  /** Throws if a vector value's length does not match the field's declared dimension. */
  public static void checkVectorDimension(
      VectorColumn<?> column, int actual, int expected, int batchDocID) {
    if (actual != expected) {
      throw new IllegalArgumentException(
          "VectorColumn \""
              + column.name()
              + "\" expected dimension "
              + expected
              + " but got vector of length "
              + actual
              + " at batch doc "
              + batchDocID);
    }
  }
}
