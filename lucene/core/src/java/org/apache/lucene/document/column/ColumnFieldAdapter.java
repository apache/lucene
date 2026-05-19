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

import java.nio.charset.StandardCharsets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Lightweight adapter that presents a {@link Column}'s current cursor value as an {@link
 * IndexableField} so it can be fed through the row-oriented indexing pass (stored fields and term
 * inversion). Holds a fresh tuple cursor over the underlying column; one instance is created per
 * column per batch.
 *
 * @lucene.internal
 */
public abstract sealed class ColumnFieldAdapter extends Field
    permits LongColumnAdapter, BinaryColumnAdapter {

  ColumnFieldAdapter(String name, IndexableFieldType fieldType) {
    super(name, fieldType);
  }

  /** Returns an adapter for the given column, dispatching on its concrete type. */
  public static ColumnFieldAdapter create(Column column) {
    if (column instanceof LongColumn lc) {
      return new LongColumnAdapter(lc);
    } else if (column instanceof BinaryColumn bc) {
      return new BinaryColumnAdapter(bc);
    } else {
      throw new IllegalArgumentException("Unknown column type: " + column.getClass().getName());
    }
  }

  /** Advances to the next batch-local doc-id with a value. */
  public abstract int nextDoc();
}

final class LongColumnAdapter extends ColumnFieldAdapter {
  private final LongTupleCursor cursor;
  private final StoredValue reusableStoredValue;
  private final StoredValue.Type storedType;

  LongColumnAdapter(LongColumn column) {
    super(column.name(), column.fieldType());
    this.cursor = column.tuples();
    if (column.fieldType().stored()) {
      this.storedType = column.storedType();
      this.reusableStoredValue = newReusableLongStoredValue(storedType);
    } else {
      this.storedType = null;
      this.reusableStoredValue = null;
    }
  }

  private static StoredValue newReusableLongStoredValue(StoredValue.Type type) {
    return switch (type) {
      case INTEGER -> new StoredValue(0);
      case LONG -> new StoredValue(0L);
      case FLOAT -> new StoredValue(0.0f);
      case DOUBLE -> new StoredValue(0.0);
      case STRING, BINARY, DATA_INPUT ->
          throw new AssertionError("rejected by ColumnValidation.validateLongColumn");
    };
  }

  @Override
  public int nextDoc() {
    return cursor.nextDoc();
  }

  @Override
  public Number numericValue() {
    return cursor.longValue();
  }

  @Override
  public StoredValue storedValue() {
    if (reusableStoredValue == null) {
      return null;
    }
    long raw = cursor.longValue();
    switch (storedType) {
      case INTEGER -> reusableStoredValue.setIntValue((int) raw);
      case LONG -> reusableStoredValue.setLongValue(raw);
      case FLOAT -> reusableStoredValue.setFloatValue(NumericUtils.sortableIntToFloat((int) raw));
      case DOUBLE -> reusableStoredValue.setDoubleValue(NumericUtils.sortableLongToDouble(raw));
      case STRING, BINARY, DATA_INPUT ->
          throw new IllegalArgumentException("rejected by ColumnValidation.validateLongColumn");
    }
    return reusableStoredValue;
  }

  @Override
  public InvertableType invertableType() {
    return null;
  }
}

final class BinaryColumnAdapter extends ColumnFieldAdapter {
  private final ObjectTupleCursor<BytesRef> cursor;
  private final StoredValue reusableStoredValue;
  private final StoredValue.Type storedType;
  private final boolean tokenized;
  private final boolean indexed;
  // Cached UTF-8 decode of the cursor's current value. Invalidated on nextDoc()
  private String cachedString;

  BinaryColumnAdapter(BinaryColumn column) {
    super(column.name(), column.fieldType());
    this.cursor = column.tuples();
    this.tokenized = column.fieldType().tokenized();
    this.indexed = column.fieldType().indexOptions() != IndexOptions.NONE;
    if (column.fieldType().stored()) {
      this.storedType = column.storedType();
      this.reusableStoredValue = newReusableStoredValue(storedType);
    } else {
      this.storedType = null;
      this.reusableStoredValue = null;
    }
  }

  private static StoredValue newReusableStoredValue(StoredValue.Type type) {
    return switch (type) {
      case STRING -> new StoredValue("");
      case BINARY -> new StoredValue(new BytesRef());
      case INTEGER, LONG, FLOAT, DOUBLE, DATA_INPUT ->
          throw new IllegalArgumentException("rejected by ColumnValidation.validateBinaryColumn");
    };
  }

  private String decodedString() {
    if (cachedString == null) {
      BytesRef ref = cursor.value();
      cachedString = new String(ref.bytes, ref.offset, ref.length, StandardCharsets.UTF_8);
    }
    return cachedString;
  }

  @Override
  public int nextDoc() {
    cachedString = null;
    return cursor.nextDoc();
  }

  @Override
  public BytesRef binaryValue() {
    return cursor.value();
  }

  @Override
  public String stringValue() {
    return tokenized ? decodedString() : null;
  }

  @Override
  public StoredValue storedValue() {
    if (reusableStoredValue == null) {
      return null;
    }
    switch (storedType) {
      case STRING -> reusableStoredValue.setStringValue(decodedString());
      case BINARY -> reusableStoredValue.setBinaryValue(cursor.value());
      case INTEGER, LONG, FLOAT, DOUBLE, DATA_INPUT ->
          throw new IllegalArgumentException("rejected by ColumnValidation.validateBinaryColumn");
    }
    return reusableStoredValue;
  }

  @Override
  public InvertableType invertableType() {
    if (indexed == false) {
      return null;
    }
    return tokenized ? InvertableType.TOKEN_STREAM : InvertableType.BINARY;
  }

  @Override
  public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
    if (tokenized) {
      return analyzer.tokenStream(name(), stringValue());
    }
    return null;
  }
}
