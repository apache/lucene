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
package org.apache.lucene.index;

/**
 * Identifies a per-block statistic that a {@link DocValuesSkipper} may provide. Each instance binds
 * an ID (used as both the .dvs type tag and the .dvm bitmask bit) with a Java return type.
 *
 * <p>Callers check availability via {@link DocValuesField#availableBlockStats()} and access values
 * via {@link DocValuesSkipper#stat(SkipStat, int)}.
 *
 * @param <T> the Java type of the stat value
 * @lucene.experimental
 */
public final class SkipStat<T> {

  private final byte id;
  private final String name;
  private final Class<T> type;

  private SkipStat(byte id, String name, Class<T> type) {
    this.id = id;
    this.name = name;
    this.type = type;
  }

  // ID 0x01 is reserved for RANGE (the mandatory base contract accessed via
  // minValue(level)/maxValue(level)/docCount(level) on DocValuesSkipper).
  // Optional stats start at 0x02.

  // Example of how optional sum stats would be defined:
  //   public static final SkipStat<Long> SUM = new SkipStat<>((byte) 0x02, "SUM", Long.class);

  private static final SkipStat<?>[] ALL = {};

  /** Returns the ID used as both the .dvs type tag and the .dvm bitmask bit position. */
  public byte id() {
    return id;
  }

  /** Returns the human-readable name. */
  public String name() {
    return name;
  }

  /** Returns the Java type of the stat value. */
  public Class<T> type() {
    return type;
  }

  /** Returns all registered SkipStat instances. */
  public static SkipStat<?>[] values() {
    return ALL.clone();
  }

  /** Resolve a SkipStat by its ID, or null if unknown. */
  public static SkipStat<?> fromId(int id) {
    for (SkipStat<?> stat : ALL) {
      if (stat.id == id) {
        return stat;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return name;
  }
}
