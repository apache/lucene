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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates, by docID, term or query for a single segment. This is used to
 * hold buffered pending deletes and updates against the to-be-flushed segment. Once the deletes and
 * updates are pushed (on flush in DocumentsWriter), they are converted to a {@link
 * FrozenBufferedUpdates} instance and pushed to the {@link BufferedUpdatesStream}.
 */

// NOTE: instances of this class are accessed either via a private
// instance on DocumentWriterPerThread, or via sync'd code by
// DocumentsWriterDeleteQueue

class BufferedUpdates implements Accountable {

  /* Rough logic: HashMap has an array[Entry] w/ varying
  load factor (say 2 * POINTER).  Entry is object w/
  Query key, Integer val, int hash, Entry next
  (OBJ_HEADER + 3*POINTER + INT).  Query we often
  undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  static final int BYTES_PER_DEL_QUERY =
      5 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 2 * Integer.BYTES
          + 24;
  final AtomicInteger numFieldUpdates = new AtomicInteger();

  final DeletedTerms deleteTerms = new DeletedTerms();
  final Map<Query, Integer> deleteQueries = new HashMap<>();

  final Map<String, FieldUpdatesBuffer> fieldUpdates = new HashMap<>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  private final Counter bytesUsed = Counter.newCounter(true);
  final Counter fieldUpdatesBytesUsed = Counter.newCounter(true);

  private static final boolean VERBOSE_DELETES = false;

  long gen;

  final String segmentName;

  public BufferedUpdates(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return ("gen=" + gen)
          + (", deleteTerms=" + deleteTerms)
          + (", deleteQueries=" + deleteQueries)
          + (", fieldUpdates=" + fieldUpdates)
          + (", bytesUsed=" + bytesUsed);
    } else {
      String s = "gen=" + gen;
      if (!deleteTerms.isEmpty()) {
        s += " " + deleteTerms.size() + " unique deleted terms ";
      }
      if (deleteQueries.size() != 0) {
        s += " " + deleteQueries.size() + " deleted queries";
      }
      if (numFieldUpdates.get() != 0) {
        s += " " + numFieldUpdates.get() + " field updates";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  public void addQuery(Query query, int docIDUpto) {
    Integer current = deleteQueries.put(query, docIDUpto);
    // increment bytes used only if the query wasn't added so far.
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
    }
  }

  public void addTerm(Term term, int docIDUpto) {
    int current = deleteTerms.get(term);
    if (current != -1 && docIDUpto < current) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.  If we blindly replace than we can
      // incorrectly get both docs indexed.
      return;
    }

    deleteTerms.put(term, docIDUpto);
  }

  void addNumericUpdate(NumericDocValuesUpdate update, int docIDUpto) {
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void addBinaryUpdate(BinaryDocValuesUpdate update, int docIDUpto) {
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void clearDeleteTerms() {
    deleteTerms.clear();
  }

  void clear() {
    deleteTerms.clear();
    deleteQueries.clear();
    numFieldUpdates.set(0);
    fieldUpdates.clear();
    bytesUsed.addAndGet(-bytesUsed.get());
    fieldUpdatesBytesUsed.addAndGet(-fieldUpdatesBytesUsed.get());
  }

  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.size() > 0 || numFieldUpdates.get() > 0;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get() + fieldUpdatesBytesUsed.get() + deleteTerms.ramBytesUsed();
  }

  static class DeletedTerms implements Accountable {

    private final Counter bytesUsed = Counter.newCounter();
    private final ByteBlockPool pool =
        new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));
    private final Map<String, BytesRefIntMap> deleteTerms = new HashMap<>();
    private int termsSize = 0;

    DeletedTerms() {}

    /**
     * Get the newest doc id of the deleted term.
     *
     * @param term The deleted term.
     * @return The newest doc id of this deleted term.
     */
    int get(Term term) {
      BytesRefIntMap hash = deleteTerms.get(term.field);
      if (hash == null) {
        return -1;
      }
      return hash.get(term.bytes);
    }

    /**
     * Put the newest doc id of the deleted term.
     *
     * @param term The deleted term.
     * @param value The newest doc id of the deleted term.
     */
    void put(Term term, int value) {
      BytesRefIntMap hash =
          deleteTerms.computeIfAbsent(
              term.field,
              k -> {
                bytesUsed.addAndGet(RamUsageEstimator.sizeOf(term.field));
                return new BytesRefIntMap(pool, bytesUsed);
              });
      if (hash.put(term.bytes, value)) {
        termsSize++;
      }
    }

    void clear() {
      pool.reset(false, false);
      bytesUsed.addAndGet(-bytesUsed.get());
      deleteTerms.clear();
      termsSize = 0;
    }

    int size() {
      return termsSize;
    }

    boolean isEmpty() {
      return termsSize == 0;
    }

    /** Just for test, not efficient. */
    Set<Term> keySet() {
      return deleteTerms.entrySet().stream()
          .flatMap(
              entry -> entry.getValue().keySet().stream().map(b -> new Term(entry.getKey(), b)))
          .collect(Collectors.toSet());
    }

    interface DeletedTermConsumer<E extends Exception> {
      void accept(Term term, int docId) throws E;
    }

    /**
     * Consume all terms in a sorted order.
     *
     * <p>Note: This is a destructive operation as it calls {@link BytesRefHash#sort()}.
     *
     * @see BytesRefHash#sort
     */
    <E extends Exception> void forEachOrdered(DeletedTermConsumer<E> consumer) throws E {
      List<Map.Entry<String, BytesRefIntMap>> deleteFields =
          new ArrayList<>(deleteTerms.entrySet());
      deleteFields.sort(Map.Entry.comparingByKey());
      Term scratch = new Term("", new BytesRef());
      for (Map.Entry<String, BufferedUpdates.BytesRefIntMap> deleteFieldEntry : deleteFields) {
        scratch.field = deleteFieldEntry.getKey();
        BufferedUpdates.BytesRefIntMap terms = deleteFieldEntry.getValue();
        int[] indices = terms.bytesRefHash.sort();
        for (int i = 0; i < terms.bytesRefHash.size(); i++) {
          int index = indices[i];
          terms.bytesRefHash.get(index, scratch.bytes);
          consumer.accept(scratch, terms.values[index]);
        }
      }
    }

    /** Visible for testing. */
    ByteBlockPool getPool() {
      return pool;
    }

    @Override
    public long ramBytesUsed() {
      return bytesUsed.get();
    }

    /** Used for {@link BufferedUpdates#VERBOSE_DELETES}. */
    @Override
    public String toString() {
      return keySet().stream()
          .map(t -> t + "=" + get(t))
          .collect(Collectors.joining(", ", "{", "}"));
    }
  }

  private static class BytesRefIntMap {

    private static final long INIT_RAM_BYTES =
        RamUsageEstimator.shallowSizeOf(BytesRefIntMap.class)
            + RamUsageEstimator.shallowSizeOf(BytesRefHash.class)
            + RamUsageEstimator.sizeOf(new int[BytesRefHash.DEFAULT_CAPACITY]);

    private final Counter counter;
    private final BytesRefHash bytesRefHash;
    private int[] values;

    private BytesRefIntMap(ByteBlockPool pool, Counter counter) {
      this.counter = counter;
      this.bytesRefHash =
          new BytesRefHash(
              pool,
              BytesRefHash.DEFAULT_CAPACITY,
              new BytesRefHash.DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, counter));
      this.values = new int[BytesRefHash.DEFAULT_CAPACITY];
      counter.addAndGet(INIT_RAM_BYTES);
    }

    private Set<BytesRef> keySet() {
      BytesRef scratch = new BytesRef();
      Set<BytesRef> set = new HashSet<>();
      for (int i = 0; i < bytesRefHash.size(); i++) {
        bytesRefHash.get(i, scratch);
        set.add(BytesRef.deepCopyOf(scratch));
      }
      return set;
    }

    private boolean put(BytesRef key, int value) {
      assert value >= 0;
      int e = bytesRefHash.add(key);
      if (e < 0) {
        values[-e - 1] = value;
        return false;
      } else {
        if (e >= values.length) {
          int originLength = values.length;
          values = ArrayUtil.grow(values, e + 1);
          counter.addAndGet((long) (values.length - originLength) * Integer.BYTES);
        }
        values[e] = value;
        return true;
      }
    }

    private int get(BytesRef key) {
      int e = bytesRefHash.find(key);
      if (e == -1) {
        return -1;
      }
      return values[e];
    }
  }
}
