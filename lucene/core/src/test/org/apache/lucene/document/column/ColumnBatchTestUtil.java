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

import java.util.List;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/** Shared helpers for column-batch indexing tests. */
public class ColumnBatchTestUtil {

  private ColumnBatchTestUtil() {}

  public static FieldType floatVectorType(int dimension, VectorSimilarityFunction sim) {
    FieldType type = new FieldType();
    type.setVectorAttributes(dimension, VectorEncoding.FLOAT32, sim);
    type.freeze();
    return type;
  }

  public static FieldType byteVectorType(int dimension, VectorSimilarityFunction sim) {
    FieldType type = new FieldType();
    type.setVectorAttributes(dimension, VectorEncoding.BYTE, sim);
    type.freeze();
    return type;
  }

  public static ColumnBatch simpleBatch(int numDocs, Column... columns) {
    return new ColumnBatch() {
      @Override
      public int numDocs() {
        return numDocs;
      }

      @Override
      public Iterable<Column> columns() {
        return List.of(columns);
      }
    };
  }

  public static class ArrayLongColumn extends LongColumn {
    private final int[] docIds;
    private final long[] values;

    public ArrayLongColumn(String name, IndexableFieldType fieldType, int[] docIds, long[] values) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    public ArrayLongColumn(
        String name,
        IndexableFieldType fieldType,
        NumericKind numericKind,
        int[] docIds,
        long[] values) {
      super(name, fieldType, Density.SPARSE, numericKind);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }
  }

  public static class ArrayBinaryColumn extends BinaryColumn {
    private final int[] docIds;
    private final BytesRef[] values;
    private final StoredValue.Type storedType;

    public ArrayBinaryColumn(
        String name, IndexableFieldType fieldType, int[] docIds, BytesRef[] values) {
      this(name, fieldType, docIds, values, StoredValue.Type.BINARY);
    }

    public ArrayBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        int[] docIds,
        BytesRef[] values,
        StoredValue.Type storedType) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
      this.storedType = storedType;
    }

    @Override
    public StoredValue.Type storedType() {
      return storedType;
    }

    @Override
    public ObjectTupleCursor<BytesRef> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef value() {
          return values[pos];
        }
      };
    }
  }

  /** Dense {@link LongColumn} with an optional bulk values cursor. */
  public static class ArrayDenseLongColumn extends LongColumn {
    private final long[] values;

    public ArrayDenseLongColumn(String name, IndexableFieldType fieldType, long[] values) {
      super(name, fieldType, Density.DENSE);
      this.values = values;
    }

    public ArrayDenseLongColumn(
        String name, IndexableFieldType fieldType, NumericKind numericKind, long[] values) {
      super(name, fieldType, Density.DENSE, numericKind);
      this.values = values;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < values.length ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }

    @Override
    public LongValuesCursor values() {
      return new LongValuesCursor(values.length) {
        int pos = 0;

        @Override
        public long nextLong() {
          if (pos >= values.length) {
            throw new IllegalStateException("LongValuesCursor exhausted: size=" + values.length);
          }
          return values[pos++];
        }

        @Override
        public void fillDocValues(long[] dst, int offset, int length) {
          if (pos + length > values.length) {
            throw new IllegalStateException("LongValuesCursor exhausted: size=" + values.length);
          }
          System.arraycopy(values, pos, dst, offset, length);
          pos += length;
        }
      };
    }
  }

  public static class ArrayFloatVectorColumn extends VectorColumn<float[]> {
    private final int[] docIds;
    private final float[][] values;

    public ArrayFloatVectorColumn(
        String name, IndexableFieldType fieldType, int[] docIds, float[][] values) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public ObjectTupleCursor<float[]> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public float[] value() {
          return values[pos];
        }
      };
    }
  }

  public static class ArrayByteVectorColumn extends VectorColumn<byte[]> {
    private final int[] docIds;
    private final byte[][] values;

    public ArrayByteVectorColumn(
        String name, IndexableFieldType fieldType, int[] docIds, byte[][] values) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
    }

    @Override
    public ObjectTupleCursor<byte[]> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public byte[] value() {
          return values[pos];
        }
      };
    }
  }

  public static class ArrayDenseFloatVectorColumn extends VectorColumn<float[]> {
    private final float[][] values;

    public ArrayDenseFloatVectorColumn(
        String name, IndexableFieldType fieldType, float[][] values) {
      super(name, fieldType, Density.DENSE);
      this.values = values;
    }

    @Override
    public ObjectTupleCursor<float[]> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < values.length ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public float[] value() {
          return values[pos];
        }
      };
    }
  }

  public static class ArrayDenseByteVectorColumn extends VectorColumn<byte[]> {
    private final byte[][] values;

    public ArrayDenseByteVectorColumn(String name, IndexableFieldType fieldType, byte[][] values) {
      super(name, fieldType, Density.DENSE);
      this.values = values;
    }

    @Override
    public ObjectTupleCursor<byte[]> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < values.length ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public byte[] value() {
          return values[pos];
        }
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Throwing-cursor helpers: each variant throws the supplied RuntimeException
  // at a designated cursor index so tests can exercise the failure-cleanup path
  // in DocumentsWriterPerThread#updateBatch.
  // ---------------------------------------------------------------------------

  /** Sparse {@link LongColumn} whose tuple cursor throws after {@code throwAt} successful calls. */
  public static class ThrowingLongColumn extends LongColumn {
    private final int[] docIds;
    private final long[] values;
    private final int throwAt;
    private final RuntimeException exception;

    public ThrowingLongColumn(
        String name,
        IndexableFieldType fieldType,
        int[] docIds,
        long[] values,
        int throwAt,
        RuntimeException exception) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
      this.throwAt = throwAt;
      this.exception = exception;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          if (pos == throwAt) {
            throw exception;
          }
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }
  }

  /**
   * Dense {@link LongColumn} whose values cursor throws partway through {@link
   * LongValuesCursor#fillDocValues}. The tuple cursor (used by row pass) is well-behaved.
   */
  public static class ThrowingDenseLongColumn extends LongColumn {
    private final long[] values;
    private final int throwAt;
    private final RuntimeException exception;

    public ThrowingDenseLongColumn(
        String name,
        IndexableFieldType fieldType,
        long[] values,
        int throwAt,
        RuntimeException exception) {
      super(name, fieldType, Density.DENSE);
      this.values = values;
      this.throwAt = throwAt;
      this.exception = exception;
    }

    @Override
    public LongTupleCursor tuples() {
      return new LongTupleCursor() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          return pos < values.length ? pos : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public long longValue() {
          return values[pos];
        }
      };
    }

    @Override
    public LongValuesCursor values() {
      return new LongValuesCursor(values.length) {
        int pos = 0;

        @Override
        public long nextLong() {
          if (pos == throwAt) {
            throw exception;
          }
          return values[pos++];
        }

        @Override
        public void fillDocValues(long[] dst, int offset, int length) {
          for (int i = 0; i < length; i++) {
            if (pos == throwAt) {
              throw exception;
            }
            dst[offset + i] = values[pos++];
          }
        }
      };
    }
  }

  /** Sparse {@link BinaryColumn} whose tuple cursor throws after {@code throwAt} calls. */
  public static class ThrowingBinaryColumn extends BinaryColumn {
    private final int[] docIds;
    private final BytesRef[] values;
    private final int throwAt;
    private final RuntimeException exception;

    public ThrowingBinaryColumn(
        String name,
        IndexableFieldType fieldType,
        int[] docIds,
        BytesRef[] values,
        int throwAt,
        RuntimeException exception) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
      this.throwAt = throwAt;
      this.exception = exception;
    }

    @Override
    public ObjectTupleCursor<BytesRef> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          if (pos == throwAt) {
            throw exception;
          }
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef value() {
          return values[pos];
        }
      };
    }
  }

  /** Sparse float {@link VectorColumn} whose tuple cursor throws after {@code throwAt} calls. */
  public static class ThrowingFloatVectorColumn extends VectorColumn<float[]> {
    private final int[] docIds;
    private final float[][] values;
    private final int throwAt;
    private final RuntimeException exception;

    public ThrowingFloatVectorColumn(
        String name,
        IndexableFieldType fieldType,
        int[] docIds,
        float[][] values,
        int throwAt,
        RuntimeException exception) {
      super(name, fieldType, Density.SPARSE);
      assert docIds.length == values.length;
      this.docIds = docIds;
      this.values = values;
      this.throwAt = throwAt;
      this.exception = exception;
    }

    @Override
    public ObjectTupleCursor<float[]> tuples() {
      return new ObjectTupleCursor<>() {
        int pos = -1;

        @Override
        public int nextDoc() {
          pos++;
          if (pos == throwAt) {
            throw exception;
          }
          return pos < docIds.length ? docIds[pos] : DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public float[] value() {
          return values[pos];
        }
      };
    }
  }
}
