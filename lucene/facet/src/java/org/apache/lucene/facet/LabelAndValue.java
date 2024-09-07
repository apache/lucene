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
package org.apache.lucene.facet;

/** Single label and its value, usually contained in a {@link FacetResult}. */
public final class LabelAndValue {
  /** Facet's label. */
  public final String label;

  /** Value associated with this label. */
  public final Number value;

  /** Constructor with unspecified count, we assume the value is a count. */
  public LabelAndValue(String label, Number value) {
    this.label = label;
    this.value = value;
  }

  @Override
  public String toString() {
    return label + " (" + value + ")";
  }

  @Override
  public boolean equals(Object _other) {
    if ((_other instanceof LabelAndValue) == false) {
      return false;
    }
    LabelAndValue other = (LabelAndValue) _other;
    if (label.equals(other.label) == false) {
      return false;
    }
    // value and other.value could be Number and ValueAndCount.
    // We need the equals in ValueAndCount to be called.
    if (value instanceof ValueAndCount) {
      return value.equals(other.value);
    }
    return other.value.equals(value);
  }

  @Override
  public int hashCode() {
    return label.hashCode() + 1439 * value.hashCode();
  }

  /**
   * Is a {@link Number} while holding a {@link Number} and a count. Meant as a value for {@link
   * LabelAndValue}.
   */
  public static class ValueAndCount extends Number {
    Number value;
    int count;

    /** All args constructor. */
    public ValueAndCount(Number value, int count) {
      this.value = value;
      this.count = count;
    }

    /** Get the count. */
    public int getCount() {
      return count;
    }

    @Override
    public int intValue() {
      return value.intValue();
    }

    @Override
    public long longValue() {
      return value.longValue();
    }

    @Override
    public float floatValue() {
      return value.floatValue();
    }

    @Override
    public double doubleValue() {
      return value.doubleValue();
    }

    @Override
    public boolean equals(Object _other) {
      if (_other instanceof ValueAndCount) {
        return value.equals(((ValueAndCount) _other).value)
            && count == ((ValueAndCount) _other).count;
      }
      if (_other instanceof Number) {
        return value.equals(_other);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public String toString() {
      return value.toString();
    }
  }
}
