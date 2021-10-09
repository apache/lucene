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
package org.apache.lucene.jmh.base.rndgen;

import com.ibm.icu.impl.Pair;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** The type Random data histogram. */
public class RandomDataHistogram {

  /** The constant MAX_TYPES_TO_COLLECT. */
  public static final int MAX_TYPES_TO_COLLECT = 500;

  /** The constant MAX_ITEMS_TO_PRINT. */
  public static final int MAX_ITEMS_TO_PRINT = 100;

  /** The constant MAX_LABEL_LENGTH. */
  public static final int MAX_LABEL_LENGTH = 100;
  /** The constant VERTICAL_BAR. */
  public static final char VERTICAL_BAR = '│';

  /** The constant COUNT. */
  public static final String COUNT = "Count";

  /** The constant MAX_WIDTH. */
  public static final int MAX_WIDTH = 120;

  /** The constant HISTO_DOT. */
  public static final char HISTO_DOT = '⦿';

  private final int bucketCnt;

  /**
   * Instantiates a new Random data histogram.
   *
   * @param bucketCnt the bucket cnt
   */
  public RandomDataHistogram(int bucketCnt) {
    this.bucketCnt = bucketCnt;
  }

  private static int compare(Tracker left, Tracker right) {
    try {
      Integer leftFirst = left.count();
      Integer rightFirst = right.count();
      return rightFirst.compareTo(leftFirst);
    } catch (ClassCastException castException) { // NOSONAR
      return -Integer.compare(left.count(), right.count());
    }
  }

  /**
   * Generate random data historgram report.
   *
   * @param entries the list of Trackers maintaining the counts
   * @param label the label for counted objects
   * @return the string
   */
  public String print(List<Tracker> entries, String label) {
    return print(entries, true, label);
  }

  /**
   * Generate random data historgram report.
   *
   * @param entries the entries
   * @param countsComparator the counts comparator
   * @param label the label
   * @return the string
   */
  public String print(List<Tracker> entries, boolean countsComparator, String label) {
    entries.sort(countsComparator ? RandomDataHistogram::compare : comparator());
    if (bucketCnt > -1) {
      return print(label, bucket(bucketCnt, entries), countsComparator);
    } else {
      List<Bucket> buckets = new ArrayList<>(32);
      for (Tracker entry : entries) {
        Bucket bucket = new Bucket(label(entry), entry.count());
        buckets.add(bucket);
      }
      return print(label, buckets, countsComparator);
    }
  }

  @SuppressWarnings("unchecked")
  private Comparator<? super Tracker> comparator() {
    if (bucketCnt > -1) {
      return (left, right) -> 0;
    }
    return (left, right) -> {
      try {
        Comparable<Object> leftFirst = (Comparable<Object>) left.values().get(0);
        Comparable<Object> rightFirst = (Comparable<Object>) right.values().get(0);
        return leftFirst.compareTo(rightFirst);
      } catch (ClassCastException castException) {
        return -Integer.compare(left.count(), right.count());
      }
    };
  }

  /** Returns the label of the tracker. */
  private String label(final Tracker tracker) {
    if (bucketCnt > -1) {
      return "<--->";
    }
    return tracker.name();
  }

  /**
   * Bucket list.
   *
   * @param bucketCnt the bucket cnt
   * @param entries the list of Trackers maintaining the counts
   * @return the list
   */
  protected static List<Bucket> bucket(int bucketCnt, final List<Tracker> entries) {
    Pair<BigInteger, BigInteger> minMax = minMax(entries);
    BigInteger min = minMax.first;
    BigInteger max = minMax.second;

    List<Pair<BigInteger, Bucket>> topsAndBuckets = bucketsAndMax(bucketCnt, min, max);

    for (Tracker entry : entries) {
      Bucket bucket = bucket(topsAndBuckets, new BigDecimal(entry.values().get(0).toString()));
      bucket.addCount(entry.count());
    }

    return topsAndBuckets.stream()
        .map(bigIntegerBucketPair -> bigIntegerBucketPair.second)
        .collect(Collectors.toList());
  }

  private String print(String label, final List<Bucket> buckets, boolean countComparator) {
    int labelWidth =
        Math.max(
            label.length(),
            buckets.stream().mapToInt(bucket2 -> bucket2.label.length()).max().orElse(0));
    int maxCnt = buckets.stream().mapToInt(bucket1 -> bucket1.count).max().orElse(0);
    int cntWidth = Math.max(COUNT.length(), (int) Math.max(1, Math.floor(Math.log10(maxCnt)) + 1));

    StringBuilder report = new StringBuilder(32);
    String header =
        "%1$4s "
            + VERTICAL_BAR
            + " %2$"
            + labelWidth
            + "s "
            + VERTICAL_BAR
            + " %3$"
            + cntWidth
            + "s "
            + VERTICAL_BAR
            + " %4$s";
    String bucket =
        "%1$4s "
            + VERTICAL_BAR
            + " %2$"
            + labelWidth
            + "s "
            + VERTICAL_BAR
            + " %3$"
            + cntWidth
            + "d "
            + VERTICAL_BAR
            + " %4$s";

    report.append("\n\n").append(this.getClass().getSimpleName()).append("\n\n");
    report.append(String.format(Locale.ROOT, header, "", label, COUNT, "")).append('\n');
    String barRuler = String.valueOf('―').repeat(Math.max(0, maxCnt + 10));
    report
        .append(String.format(Locale.ROOT, header, "", "", "", barRuler).replace(" ", "―"))
        .append('\n');

    int size = buckets.size();

    for (int i = 0; i < size; i++) {
      report
          .append(
              String.format(
                  Locale.ROOT,
                  bucket,
                  i,
                  buckets.get(i).label,
                  buckets.get(i).count,
                  String.valueOf(HISTO_DOT)
                      .repeat(
                          Math.max(
                              0,
                              (int)
                                  (buckets.get(i).count
                                      / Math.max(1.0, maxCnt / (double) MAX_WIDTH))))))
          .append('\n');
    }
    report.append('\n');
    return report.toString();
  }

  private static Bucket bucket(List<Pair<BigInteger, Bucket>> bucketsAndMax, BigDecimal value) {
    int size = bucketsAndMax.size();
    for (int i = 0; i < size; i++) {
      Pair<BigInteger, Bucket> bucketAndMax = bucketsAndMax.get(i);
      BigInteger top = bucketAndMax.first;
      if (value.compareTo(new BigDecimal(top)) < 0) {
        return bucketAndMax.second;
      }
      if (i == size - 1) {
        return bucketAndMax.second;
      }
    }
    throw new IllegalArgumentException("No bucket found for value: " + value);
  }

  private static List<Pair<BigInteger, Bucket>> bucketsAndMax(
      int bucketCnt, final BigInteger min, final BigInteger max) {
    BigInteger range = max.subtract(min);
    BigInteger numberOfBuckets = BigInteger.valueOf(bucketCnt);
    BigInteger step = range.divide(numberOfBuckets);
    BigInteger remainder = range.remainder(numberOfBuckets);
    if (remainder.compareTo(BigInteger.ZERO) != 0) {
      step = step.add(BigInteger.ONE);
    }

    List<Pair<BigInteger, Bucket>> bucketsAndMax = new ArrayList<>(32);
    BigInteger left = min;
    for (BigInteger index = min.add(step); index.compareTo(max) < 0; index = index.add(step)) {
      String label = String.format(Locale.ROOT, "[%s..%s", left, index) + '[';
      bucketsAndMax.add(Pair.of(index, new Bucket(label)));
      left = index;
    }
    String label = String.format(Locale.ROOT, "[%s..%s", left, max) + ']';
    bucketsAndMax.add(Pair.of(max, new Bucket(label)));
    return bucketsAndMax;
  }

  private static Pair<BigInteger, BigInteger> minMax(final List<Tracker> entries) {
    BigDecimal min = null;
    BigDecimal max = null;
    for (Tracker entry : entries) {
      try {
        BigDecimal value = new BigDecimal(entry.values().get(0).toString());
        if (min == null || value.compareTo(min) < 0) {
          min = value;
        }
        if (max == null || value.compareTo(max) > 0) {
          max = value;
        }
      } catch (NumberFormatException e) {
        throw new ParseNumberException("Could not parse " + entry.values().get(0), e);
      }
    }
    if (max == null) max = new BigDecimal(0);
    if (min == null) min = new BigDecimal(0);
    // TODO: rounds to 4 decimals but leaves tons of trailing 0's
    // max = max.round(MATH_CONTEXT);
    // max = max.round(MATH_CONTEXT);
    BigInteger maxBigInteger = max.setScale(0, RoundingMode.UP).toBigInteger();
    return Pair.of(min.toBigInteger(), maxBigInteger);
  }

  /** The type Bucket. */
  public static class Bucket {
    private final String label;
    private int count;

    /**
     * Instantiates a new Bucket.
     *
     * @param label the label
     */
    public Bucket(String label) {
      this(label, 0);
    }

    /**
     * Instantiates a new Bucket.
     *
     * @param label the label
     * @param count the count
     */
    public Bucket(String label, final int count) {
      Objects.requireNonNull(label);
      this.label = label;
      this.count = count;
    }

    /**
     * Add count.
     *
     * @param count the count
     */
    void addCount(int count) {
      this.count += count;
    }
  }

  /** The type Counts. */
  public static class Counts {

    /** The constant ITEMS_ONLY_PRINTING. */
    public static final String ITEMS_ONLY_PRINTING = " items, only printing ";

    /** The constant THERE_ARE_OVER. */
    public static final String THERE_ARE_OVER = "\n\nThere are over ";

    /** The constant ELIPSE. */
    public static final String ELIPSE = " ... ";

    private final Map<List<Surrogate>, AtomicInteger> objectToCount = new ConcurrentHashMap<>(64);
    private final String label;
    private List<Tracker> entries = null;
    private volatile boolean limitReached;

    private final boolean bucketed;

    /**
     * Instantiates a new Counts.
     *
     * @param label the label
     * @param bucketed the bucketed
     */
    public Counts(String label, boolean bucketed) {
      this.label = label;
      this.bucketed = bucketed;
    }

    /**
     * Collect counts.
     *
     * @param values the values
     */
    public void collect(Object... values) {
      if (values.length == 0) {
        throw new IllegalArgumentException("values length must be greater than 0: " + label);
      }

      if (objectToCount.size() > MAX_ITEMS_TO_PRINT) {
        limitReached = true;
        return;
      }

      List<Surrogate> key;
      key = surrogates(values);

      AtomicInteger count = objectToCount.computeIfAbsent(key, any -> new AtomicInteger(0));
      count.incrementAndGet();
      entries = null;
    }

    private List<Surrogate> surrogates(Object[] values) {
      List<Surrogate> surrogates = new ArrayList<>(values.length);
      for (Object value : values) {
        String toString = value.toString();
        surrogates.add(
            new Surrogate(
                value.hashCode(),
                System.identityHashCode(value),
                toString.length() > MAX_LABEL_LENGTH
                    ? toString.substring(0, MAX_LABEL_LENGTH) + " ..."
                    : toString));
      }
      return surrogates;
    }

    /** The type Surrogate. */
    static class Surrogate {
      /** The Hash code. */
      final Integer hashCode;

      /** The Identity hashcode. */
      final Integer identityHashcode;

      /** The Value. */
      final String value;

      @Override
      public String toString() {
        return value;
      }

      /**
       * Instantiates a new Surrogate.
       *
       * @param hash the hash
       * @param identity the identity
       * @param displayValue the display value
       */
      Surrogate(Integer hash, Integer identity, String displayValue) {
        this.hashCode = hash;
        this.identityHashcode = identity;
        this.value = displayValue;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Surrogate surrogate = (Surrogate) o;
        return hashCode.equals(surrogate.hashCode)
            && identityHashcode.equals(surrogate.identityHashcode);
      }

      @Override
      public int hashCode() {

        if (hashCode == null && identityHashcode == null) {
          return 0;
        }

        if (hashCode == null) {
          return 0;
        }

        int result = 1;

        result = 31 * result + hashCode.hashCode();

        result = 31 * result + (identityHashcode == null ? 0 : identityHashcode.hashCode());

        return result;
      }
    }

    /** Print a string representation of the random data histogram. @return the string */
    public String print() {
      return print(label.endsWith("(ZIPFIAN)"), !this.bucketed ? -1 : 15);
    }

    /**
     * Print a string representation of the random data histogram.
     *
     * @param bucketed -1 for no bucketing or the number of buckets
     * @return the string
     */
    public String print(int bucketed) {
      return print(label.endsWith("(ZIPFIAN)"), bucketed);
    }

    /**
     * Print string.
     *
     * @param sortByCount the sort by label
     * @param bucketed the bucketed
     * @return the string
     */
    public String print(boolean sortByCount, int bucketed) {
      String msg = "";
      if (entries == null) {
        Collection<AtomicInteger> values = objectToCount.values();
        if (limitReached) {
          msg =
              THERE_ARE_OVER
                  + MAX_ITEMS_TO_PRINT
                  + ITEMS_ONLY_PRINTING
                  + MAX_ITEMS_TO_PRINT
                  + ELIPSE;
        }
        int sum = values.stream().mapToInt(AtomicInteger::get).sum();
        entries =
            objectToCount.entrySet().stream()
                .sorted(
                    (e1, e2) -> {
                      List<Surrogate> k1 = e1.getKey();
                      List<Surrogate> k2 = e2.getKey();

                      if (k1.size() != k2.size()) {
                        return Integer.compare(k1.size(), k2.size());
                      }
                      return Integer.compare(e2.getValue().get(), e1.getValue().get());
                    })
                .filter(entry -> !entry.getKey().equals(Collections.emptyList()))
                .map(
                    entry -> {
                      double percentage = entry.getValue().get() * 100.0 / sum;
                      return new Tracker(
                          entry.getKey(),
                          entry.getKey().stream()
                              .map(Objects::toString)
                              .collect(Collectors.joining(" ")),
                          entry.getValue().get(),
                          percentage);
                    })
                .collect(Collectors.toList());
      }

      RandomDataHistogram randomDataHistogram = new RandomDataHistogram(bucketed);
      return msg + '\n' + randomDataHistogram.print(entries, sortByCount, label);
    }
  }

  private static class ParseNumberException extends RuntimeException {
    /**
     * Instantiates a new Parse number exception.
     *
     * @param s the s
     * @param e the e
     */
    public ParseNumberException(String s, NumberFormatException e) {
      super(s, e);
    }
  }
}
