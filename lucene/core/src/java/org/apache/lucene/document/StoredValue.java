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
package org.apache.lucene.document;

import java.util.Objects;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

/**
 * Abstraction around a stored value.
 *
 * @see IndexableField
 */
public final class StoredValue {

  /** Type of a {@link StoredValue}. */
  public enum Type {
    /** Type of integer values. */
    INTEGER,
    /** Type of long values. */
    LONG,
    /** Type of float values. */
    FLOAT,
    /** Type of double values. */
    DOUBLE,
    /** Type of binary values. */
    BINARY,
    /** Type of string values. */
    STRING;
  }

  private final Type type;
  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private BytesRef binaryValue;
  private String stringValue;

  /** Ctor for integer values. */
  public StoredValue(int value) {
    type = Type.INTEGER;
    intValue = value;
  }

  /** Ctor for long values. */
  public StoredValue(long value) {
    type = Type.LONG;
    longValue = value;
  }

  /** Ctor for float values. */
  public StoredValue(float value) {
    type = Type.FLOAT;
    floatValue = value;
  }

  /** Ctor for double values. */
  public StoredValue(double value) {
    type = Type.DOUBLE;
    doubleValue = value;
  }

  /** Ctor for binary values. */
  public StoredValue(BytesRef value) {
    type = Type.BINARY;
    binaryValue = Objects.requireNonNull(value);
  }

  /** Ctor for binary values. */
  public StoredValue(String value) {
    type = Type.STRING;
    stringValue = Objects.requireNonNull(value);
  }

  /** Retrieve the type of the stored value. */
  public Type getType() {
    return type;
  }

  /** Set an integer value. */
  public void setIntValue(int value) {
    if (type != Type.INTEGER) {
      throw new IllegalArgumentException("Cannot set an integer on a " + type + " value");
    }
    intValue = value;
  }

  /** Set a long value. */
  public void setLongValue(long value) {
    if (type != Type.LONG) {
      throw new IllegalArgumentException("Cannot set a long on a " + type + " value");
    }
    longValue = value;
  }

  /** Set a float value. */
  public void setFloatValue(float value) {
    if (type != Type.FLOAT) {
      throw new IllegalArgumentException("Cannot set a float on a " + type + " value");
    }
    floatValue = value;
  }

  /** Set a double value. */
  public void setDoubleValue(double value) {
    if (type != Type.DOUBLE) {
      throw new IllegalArgumentException("Cannot set a double on a " + type + " value");
    }
    doubleValue = value;
  }

  /** Set a binary value. */
  public void setBinaryValue(BytesRef value) {
    if (type != Type.BINARY) {
      throw new IllegalArgumentException("Cannot set a binary value on a " + type + " value");
    }
    binaryValue = Objects.requireNonNull(value);
  }

  /** Set a string value. */
  public void setStringValue(String value) {
    if (type != Type.STRING) {
      throw new IllegalArgumentException("Cannot set a string value on a " + type + " value");
    }
    stringValue = Objects.requireNonNull(value);
  }

  /** Retrieve an integer value. */
  public int getIntValue() {
    if (type != Type.INTEGER) {
      throw new IllegalArgumentException("Cannot get an integer on a " + type + " value");
    }
    return intValue;
  }

  /** Retrieve a long value. */
  public long getLongValue() {
    if (type != Type.LONG) {
      throw new IllegalArgumentException("Cannot get a long on a " + type + " value");
    }
    return longValue;
  }

  /** Retrieve a float value. */
  public float getFloatValue() {
    if (type != Type.FLOAT) {
      throw new IllegalArgumentException("Cannot get a float on a " + type + " value");
    }
    return floatValue;
  }

  /** Retrieve a double value. */
  public double getDoubleValue() {
    if (type != Type.DOUBLE) {
      throw new IllegalArgumentException("Cannot get a double on a " + type + " value");
    }
    return doubleValue;
  }

  /** Retrieve a binary value. */
  public BytesRef getBinaryValue() {
    if (type != Type.BINARY) {
      throw new IllegalArgumentException("Cannot get a binary value on a " + type + " value");
    }
    return binaryValue;
  }

  /** Retrieve a string value. */
  public String getStringValue() {
    if (type != Type.STRING) {
      throw new IllegalArgumentException("Cannot get a string value on a " + type + " value");
    }
    return stringValue;
  }
}
