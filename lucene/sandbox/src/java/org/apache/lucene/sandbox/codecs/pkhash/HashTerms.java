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
package org.apache.lucene.sandbox.codecs.pkhash;

import static org.apache.lucene.sandbox.codecs.pkhash.TermHash.POINTER_SHIFT;
import static org.apache.lucene.sandbox.codecs.pkhash.TermHash.TAG_MASK;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.HashFieldsProducer.FieldEntry;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The {@link Terms} of a field written by {@link PrimaryKeyHashPostingsFormat} with a hash table.
 * Statistics, ordered iteration and {@link #intersect} come from the delegate format; exact seeks
 * on its {@link #iterator() TermsEnum} are answered by the hash table, except for keys with more
 * than one document ({@link #MULTI}), which are then sought in the delegate's terms dictionary. The
 * field's filter, if it has one, is only read by {@link #lookupBatch}.
 *
 * <p>{@link #lookupBatch} resolves many keys at once. Callers can detect it with {@code instanceof}
 * on the {@code Terms} of a leaf reader; a reader wrapper may hide it, in which case a {@link
 * TermsEnum} should be used instead.
 *
 * @lucene.experimental
 */
public final class HashTerms extends Terms {

  /** {@link #lookupBatch} result for a key that does not occur in this segment. */
  public static final int ABSENT = -1;

  /**
   * {@link #lookupBatch} result for a key that occurs in more than one document of this segment
   * (including deleted ones); its documents must be read with a {@link TermsEnum}.
   */
  public static final int MULTI = -2;

  /** Keys are resolved in chunks of this many keys by {@link #lookupBatch}. */
  static final int CHUNK_SIZE = 32;

  /** Internal result of {@link #matchRecord}: the record holds another key. */
  static final int NO_MATCH = Integer.MIN_VALUE;

  private final Terms delegate;

  /** This field's records and slots; never read itself, only cloned. */
  private final IndexInput slice;

  /** The smallest and largest terms of the field. */
  private final BytesRef min, max;

  /**
   * Where keys are compared with {@link #min} and {@link #max} from: 0, unless the two share their
   * first 8 bytes, as keys of a scheme such as {@code https://} do, in which case it is the length
   * of their common prefix. Every key between them starts with that prefix too, so a key that does
   * not is out of range, and the others are compared with them on the bytes after it.
   */
  private final int rangeOffset;

  /** The {@link #prefix} of {@link #min} and that of {@link #max}, from {@link #rangeOffset} on. */
  private final long minPrefix, maxPrefix;

  private final int partitionBits;
  private final long[] controlOffsets;

  /** Where the slot words of every partition start, if the records are in term order; or null. */
  private final long[] slotsOffsets;

  /** Where the inline records of every partition start, if the field's keys are inline; or null. */
  private final long[] recordsOffsets;

  /** The length of an inline record, 4 + {@link #keyLength}; 0 if the records are in term order. */
  private final int recordStride;

  /** The length of every key of the field, if its keys are inline; else -1. */
  private final int keyLength;

  private final int[] capacities; // home slots per partition
  private final int[] slotCounts; // home slots plus the tail after them
  private final int[] maxProbes;

  /** Where the filter starts in the slice, and its number of words, 0 if there is no filter. */
  private final long filterOffset;

  private final int filterWords;

  /**
   * Creates the terms of a field with a table. {@code min} and {@code max} are the smallest and
   * largest terms of {@code delegate}, which a field with a table always has.
   */
  HashTerms(Terms delegate, BytesRef min, BytesRef max, FieldEntry entry, IndexInput data)
      throws IOException {
    this.delegate = delegate;
    this.slice = data.slice("hash " + entry.field, entry.sliceOffset, entry.sliceLength);
    // Kept here, as BlockTree does, because comparing with them rules out keys without any read.
    this.min = BytesRef.deepCopyOf(min);
    this.max = BytesRef.deepCopyOf(max);
    this.rangeOffset = rangeOffset(this.min, this.max);
    this.minPrefix = prefix(this.min.bytes, rangeOffset, this.min.length - rangeOffset);
    this.maxPrefix = prefix(this.max.bytes, rangeOffset, this.max.length - rangeOffset);
    this.partitionBits = entry.partitionBits;
    this.controlOffsets = entry.controlOffsets;
    this.slotsOffsets = entry.slotsOffsets;
    this.recordsOffsets = entry.recordsOffsets;
    if (entry.keyLength != 0) {
      this.keyLength = entry.keyLength;
      this.recordStride = Integer.BYTES + entry.keyLength;
    } else {
      this.keyLength = -1;
      this.recordStride = 0;
    }
    this.capacities = entry.capacities;
    this.slotCounts = entry.slotCounts;
    this.maxProbes = entry.maxProbes;
    this.filterOffset = entry.filterOffset;
    this.filterWords = entry.filterWords;
  }

  /**
   * The {@link #rangeOffset} of a field whose smallest and largest terms are {@code min} and {@code
   * max}, which start at offset 0. Package-private for tests.
   */
  static int rangeOffset(BytesRef min, BytesRef max) {
    assert min.offset == 0 && max.offset == 0;
    final int mismatch = Arrays.mismatch(min.bytes, 0, min.length, max.bytes, 0, max.length);
    final int common = mismatch < 0 ? min.length : mismatch;
    return common < Long.BYTES ? 0 : common;
  }

  /** The {@link #rangeOffset} of this field. For tests. */
  int rangeOffset() {
    return rangeOffset;
  }

  /**
   * Returns the terms of the delegate format, whose {@link TermsEnum} seeks keys in the terms
   * dictionary. A key that {@link #lookupBatch} found to have several documents ({@link #MULTI}) is
   * best sought there: this field's own {@link #iterator() TermsEnum} would look it up in the table
   * again first.
   */
  public Terms getDelegate() {
    return delegate;
  }

  /** Number of partitions of the table. For tests. */
  int numPartitions() {
    return controlOffsets.length;
  }

  /** Whether the field's keys are inline, all of {@link #keyLength} bytes. For tests. */
  boolean isInline() {
    return recordStride != 0;
  }

  /** The length of every key of the field, if its keys are inline, else -1. For tests. */
  int keyLength() {
    return keyLength;
  }

  /** The length of an inline record, 0 if the field's records are in term order. For tests. */
  int recordStride() {
    return recordStride;
  }

  /**
   * Whether the field cannot hold a key of {@code length} bytes, which is then known to be absent
   * without any read: its keys are inline, and of another length.
   */
  boolean excludesLength(int length) {
    return keyLength >= 0 && length != keyLength;
  }

  /** Number of home slots of a partition. For tests. */
  int capacityOfPartition(int p) {
    return capacities[p];
  }

  /** Number of slots of a partition: its home slots and the tail after them. For tests. */
  int slotCountOfPartition(int p) {
    return slotCounts[p];
  }

  /** Largest displacement of a key from its home slot in a partition. For tests. */
  int maxProbeOfPartition(int p) {
    return maxProbes[p];
  }

  /** Whether the field has a filter, which {@link #lookupBatch} reads first. For tests. */
  boolean hasFilter() {
    return filterWords != 0;
  }

  /** The number of words of the filter, 0 if there is none. For tests. */
  int filterWords() {
    return filterWords;
  }

  /** Where the filter starts in the slice. For tests. */
  long filterOffset() {
    return filterOffset;
  }

  /**
   * The {@link TermHash#filterHash(long, int) filter hash} in this field of a key whose hash is
   * {@code hash} and whose {@link TermHash#mix mix} is {@code mixed}.
   */
  long filterHash(long mixed, long hash) {
    return TermHash.filterHash(mixed, hash, partitionBits);
  }

  /**
   * Position in the slice of the word of the filter of a key whose filter hash in this field
   * ({@link #filterHash}) is {@code filterHash}. The field must have a filter. For tests.
   */
  long filterWordPosition(long filterHash) {
    assert filterWords != 0;
    return filterOffset + BlockedBloomFilter.wordOffset(filterHash, filterWords);
  }

  /**
   * A new random-access view of this field's slice. {@link RandomAccessInput} is not thread-safe,
   * so every enum and every batch gets its own clone, which is cheaper than slicing the data file
   * again.
   */
  RandomAccessInput newSlice() throws IOException {
    IndexInput clone = slice.clone();
    return clone instanceof RandomAccessInput rai
        ? rai
        : clone.randomAccessSlice(0, clone.length());
  }

  /**
   * Whether a key sorts before the field's smallest term or after its largest one, so that it is
   * known to be absent without reading anything. The key's 8 bytes from {@link #rangeOffset} are
   * compared with those of the two terms, and the whole key only with a term whose 8 bytes are the
   * same.
   */
  boolean outOfRange(byte[] key, int offset, int length) {
    final long prefix;
    if (rangeOffset != 0) {
      if (length < rangeOffset
          || Arrays.equals(key, offset, offset + rangeOffset, min.bytes, 0, rangeOffset) == false) {
        return true; // it does not start with the prefix of every key in the range
      }
      prefix = prefix(key, offset + rangeOffset, length - rangeOffset);
    } else {
      prefix = prefix(key, offset, length);
    }
    return compare(prefix, key, offset, length, minPrefix, min) < 0
        || compare(prefix, key, offset, length, maxPrefix, max) > 0;
  }

  /**
   * The first 8 bytes of a key as a big-endian long, followed by zero bytes if the key is shorter.
   *
   * <p>Keys whose prefixes differ are in the order of their prefixes compared as unsigned longs: at
   * the first byte where the prefixes differ, either both keys have a byte, which is their first
   * difference, or one key is padded and the other has a non-zero byte, so the padded key is a
   * proper prefix of the other and smaller in both orders. Equal prefixes say nothing of the order,
   * as for keys that differ after 8 bytes, or {@code [0x61]} and {@code [0x61, 0x00]}.
   */
  static long prefix(byte[] key, int offset, int length) {
    if (length >= Long.BYTES) {
      return (long) BitUtil.VH_BE_LONG.get(key, offset);
    }
    long prefix = 0;
    for (int i = 0; i < length; i++) {
      prefix |= (key[offset + i] & 0xFFL) << (Long.SIZE - Byte.SIZE * (i + 1));
    }
    return prefix;
  }

  /**
   * Compares a key with a term as {@link Arrays#compareUnsigned(byte[], int, int, byte[], int,
   * int)} does, given the {@link #prefix} of both, or that of their bytes from an offset before
   * which they are the same. Package-private for tests.
   */
  static int compare(
      long keyPrefix, byte[] key, int offset, int length, long termPrefix, BytesRef term) {
    if (keyPrefix != termPrefix) {
      return Long.compareUnsigned(keyPrefix, termPrefix);
    }
    return Arrays.compareUnsigned(
        key, offset, offset + length, term.bytes, term.offset, term.offset + term.length);
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new HashTermsEnum(this);
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    return delegate.intersect(compiled, startTerm);
  }

  @Override
  public long size() throws IOException {
    return delegate.size();
  }

  @Override
  public long getSumTotalTermFreq() {
    return delegate.getSumTotalTermFreq();
  }

  @Override
  public long getSumDocFreq() {
    return delegate.getSumDocFreq();
  }

  @Override
  public int getDocCount() {
    return delegate.getDocCount();
  }

  @Override
  public boolean hasFreqs() {
    return delegate.hasFreqs();
  }

  @Override
  public boolean hasOffsets() {
    return delegate.hasOffsets();
  }

  @Override
  public boolean hasPositions() {
    return delegate.hasPositions();
  }

  @Override
  public boolean hasPayloads() {
    return delegate.hasPayloads();
  }

  @Override
  public BytesRef getMin() throws IOException {
    return min;
  }

  @Override
  public BytesRef getMax() throws IOException {
    return max;
  }

  @Override
  public Object getStats() throws IOException {
    return delegate.getStats();
  }

  /**
   * Looks up the keys {@code [from, to)} of {@code keys} and writes, for each key {@code i}, to
   * {@code docs[i - from]} either the id of the only document of this segment that contains it,
   * {@link #ABSENT}, or {@link #MULTI}. Deleted documents are not filtered out.
   *
   * <p>Keys that sort before the field's smallest term ({@link #getMin}) or after its largest one
   * ({@link #getMax}) are found absent, but only after a lookup: a caller with sorted keys, such as
   * {@link org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery}, passes only those between
   * the two. If the field has a filter, it is read before the table. It pays off when most keys are
   * absent: present keys always pass it, so for them its reads only add to those of the table.
   *
   * <p>Keys are looked up in the table in batches of {@link #CHUNK_SIZE}, in stages. No read of a
   * stage depends on another, so their cache misses overlap instead of being paid one after the
   * other.
   *
   * <p>Allocates a few small arrays per call and nothing per key. Thread-safe.
   *
   * @throws IndexOutOfBoundsException if {@code [from, to)} is not a range of {@code keys}
   * @throws IllegalArgumentException if {@code docs} is shorter than {@code to - from}
   */
  public void lookupBatch(HashedTerms keys, int from, int to, int[] docs) throws IOException {
    Objects.checkFromToIndex(from, to, keys.size());
    if (docs.length < to - from) {
      throw new IllegalArgumentException(
          "docs has length " + docs.length + " but " + (to - from) + " keys were requested");
    }
    if (from == to) {
      return;
    }
    final RandomAccessInput in = newSlice();
    final Batch batch = new Batch(Math.min(CHUNK_SIZE, to - from));
    if (filterWords != 0) {
      lookupWithFilter(in, keys, from, to, docs, from, batch);
    } else {
      for (int i = from; i < to; i++) {
        if (excludesLength(keys.length(i))) {
          docs[i - from] = ABSENT;
        } else {
          batch.keys[batch.size++] = i;
          if (batch.size == batch.capacity()) {
            lookupInTable(in, keys, batch, docs, from);
          }
        }
      }
    }
    if (batch.size > 0) {
      lookupInTable(in, keys, batch, docs, from);
    }
  }

  /**
   * Checks the keys {@code [from, to)} against the field's filter, in chunks of {@link #CHUNK_SIZE}
   * keys, and adds the keys that pass to {@code batch}, which is looked up in the table whenever it
   * is full; the others are absent. The result of key {@code i} is written to {@code docs[i -
   * origin]}. Since the batch only fills up with keys that pass, the stages of the table have as
   * many keys to overlap as without a filter. What the filter needs of a key that does not depend
   * on the segment is computed once, by {@link HashedTerms}.
   */
  private void lookupWithFilter(
      RandomAccessInput in, HashedTerms keys, int from, int to, int[] docs, int origin, Batch batch)
      throws IOException {
    final long[] hashes = keys.hashes();
    final long[] mixes = keys.mixes();
    final long[] masks = keys.filterMasks();
    // for the keys of a chunk that may be in the field: their indices, where their words are in
    // the filter, and the words
    final int[] chunk = new int[batch.capacity()];
    final long[] offsets = new long[batch.capacity()];
    final long[] words = new long[batch.capacity()];
    for (int base = from; base < to; base += CHUNK_SIZE) {
      final int end = Math.min(base + CHUNK_SIZE, to);
      int n = 0;
      for (int i = base; i < end; i++) {
        docs[i - origin] = ABSENT; // unless the table finds it
        if (excludesLength(keys.length(i)) == false) {
          chunk[n++] = i;
        }
      }
      // Stage 0: the filter word of every key. No read depends on another, or on a branch.
      for (int j = 0; j < n; j++) {
        final int i = chunk[j];
        offsets[j] = BlockedBloomFilter.wordOffset(filterHash(mixes[i], hashes[i]), filterWords);
      }
      for (int j = 0; j < n; j++) {
        words[j] = in.readLong(filterOffset + offsets[j]);
      }
      // Which keys pass is unpredictable, so they are added to the batch without a branch.
      for (int j = 0; j < n; j++) {
        final int i = chunk[j];
        batch.keys[batch.size] = i;
        batch.size += BlockedBloomFilter.mayContain(words[j], masks[i]);
        if (batch.size == batch.capacity()) {
          lookupInTable(in, keys, batch, docs, origin);
        }
      }
    }
  }

  /**
   * Keys to look up in the table together, by their index in a {@link HashedTerms}, and scratch.
   */
  private static final class Batch {
    final int[] keys;
    int size;
    final int[] partitions;
    final int[] homes;
    final int[] slots;
    final long[] controlPositions;
    final long[] controls;
    final long[] words;
    final int[] candidates;
    final int[] pending;
    final long[] heads;
    final long[] tails; // of inline keys only

    Batch(int capacity) {
      keys = new int[capacity];
      partitions = new int[capacity];
      homes = new int[capacity];
      slots = new int[capacity];
      controlPositions = new long[capacity];
      controls = new long[capacity];
      words = new long[capacity];
      candidates = new int[capacity];
      pending = new int[capacity];
      heads = new long[capacity];
      tails = new long[capacity];
    }

    int capacity() {
      return keys.length;
    }
  }

  /**
   * Looks up the keys of a batch in the table, writing the result of key {@code i} to {@code docs[i
   * - origin]}, and empties the batch. On a field whose keys are inline, every key of the batch has
   * their length.
   */
  private void lookupInTable(
      RandomAccessInput in, HashedTerms keys, Batch batch, int[] docs, int origin)
      throws IOException {
    final int numCandidates = findCandidates(in, keys, batch, docs, origin);
    if (recordStride != 0) {
      matchInlineRecords(in, keys, batch, numCandidates, docs, origin);
    } else {
      matchRecords(in, keys, batch, numCandidates, docs, origin);
    }
    batch.size = 0;
  }

  /**
   * Stages 1 and 2 of {@link #lookupInTable}. Stage 1 reads the 8 control bytes from every key's
   * home slot, in a loop that does nothing else. Stage 2 scans them to each key's first candidate
   * slot; most misses end here, at an empty slot. The keys without a candidate are absent; returns
   * how many have one, which the first entries of {@code batch.candidates} are, by their index in
   * the batch.
   */
  private int findCandidates(
      RandomAccessInput in, HashedTerms keys, Batch batch, int[] docs, int origin)
      throws IOException {
    final int n = batch.size;
    final int[] indices = batch.keys;
    final int[] partitions = batch.partitions;
    final int[] homes = batch.homes;
    final long[] controlPositions = batch.controlPositions;
    final long[] controls = batch.controls;
    for (int j = 0; j < n; j++) {
      final long hash = keys.hash(indices[j]);
      final int p = TermHash.partition(hash, partitionBits);
      final int home = TermHash.homeSlot(hash, partitionBits, capacities[p]);
      partitions[j] = p;
      homes[j] = home;
      controlPositions[j] = controlOffsets[p] + home;
    }
    for (int j = 0; j < n; j++) {
      controls[j] = in.readLong(controlPositions[j]);
    }

    int numCandidates = 0;
    for (int j = 0; j < n; j++) {
      final int p = partitions[j];
      final int home = homes[j];
      final int slot =
          findCandidate(
              in,
              controlOffsets[p],
              home,
              controls[j],
              limit(p, home),
              repeat(keys.control(indices[j])));
      if (slot >= 0) {
        batch.slots[j] = slot;
        batch.candidates[numCandidates++] = j;
      } else {
        docs[indices[j] - origin] = ABSENT;
      }
    }
    return numCandidates;
  }

  /**
   * Stages 3 to 5 of {@link #lookupInTable}, on a field whose records are in term order, for the
   * first {@code numCandidates} keys of {@code batch.candidates}. In rounds until no key has a
   * candidate left, stage 3 reads the slot of every candidate, and stage 4 checks tags and reads
   * the first 8 bytes of the record of every candidate whose tag matches. A control byte also
   * matches another key's once per 128 slots or so: such a key scans on, and its next candidate is
   * read in the next round, with the others'. Stage 5 compares keys. A tag also matches another
   * key's once per 2^16 or so; the scan then resumes one key at a time.
   */
  private void matchRecords(
      RandomAccessInput in,
      HashedTerms keys,
      Batch batch,
      int numCandidates,
      int[] docs,
      int origin)
      throws IOException {
    final byte[] bytes = keys.bytes();
    final int[] indices = batch.keys;
    final int[] partitions = batch.partitions;
    final int[] homes = batch.homes;
    final int[] slots = batch.slots;
    final long[] words = batch.words;
    final int[] candidates = batch.candidates;
    final int[] pending = batch.pending;
    final long[] heads = batch.heads;
    int numPending = 0;
    while (numCandidates > 0) {
      for (int c = 0; c < numCandidates; c++) {
        final int j = candidates[c];
        words[j] = in.readLong(slotsOffsets[partitions[j]] + ((long) slots[j] << 3));
      }
      int numNext = 0;
      for (int c = 0; c < numCandidates; c++) {
        final int j = candidates[c];
        final int i = indices[j];
        final long word = words[j];
        if ((word & TAG_MASK) == keys.tag(i)) {
          heads[numPending] = in.readLong(recordPosition(word));
          pending[numPending++] = j;
        } else {
          final int slot = nextCandidate(in, partitions[j], homes[j], slots[j], keys.control(i));
          if (slot >= 0) {
            slots[j] = slot;
            candidates[numNext++] = j; // never after c, which is done with
          } else {
            docs[i - origin] = ABSENT;
          }
        }
      }
      numCandidates = numNext;
    }

    for (int c = 0; c < numPending; c++) {
      final int j = pending[c];
      final int i = indices[j];
      final int offset = keys.offset(i);
      final int length = keys.length(i);
      int doc = matchRecord(in, recordPosition(words[j]), heads[c], bytes, offset, length);
      if (doc == NO_MATCH) {
        doc =
            probe(
                in,
                partitions[j],
                homes[j],
                slots[j] + 1,
                keys.tag(i),
                keys.control(i),
                bytes,
                offset,
                length);
      }
      docs[i - origin] = doc;
    }
  }

  /**
   * Stages 3 and 4 of {@link #lookupInTable}, on a field whose keys are inline, for the first
   * {@code numCandidates} keys of {@code batch.candidates}, in rounds until no key has a candidate
   * left. Stage 3 reads the first 8 bytes of the record of every candidate and, for keys of more
   * than 4 bytes, its last 8, which cover both cache lines of a record that crosses one. Stage 4
   * compares keys; a key whose candidate holds another key scans on, and its next candidate's
   * record is read in the next round.
   */
  private void matchInlineRecords(
      RandomAccessInput in,
      HashedTerms keys,
      Batch batch,
      int numCandidates,
      int[] docs,
      int origin)
      throws IOException {
    final byte[] bytes = keys.bytes();
    final int keyLength = this.keyLength;
    final int[] indices = batch.keys;
    final int[] partitions = batch.partitions;
    final int[] homes = batch.homes;
    final int[] slots = batch.slots;
    final long[] heads = batch.heads;
    final long[] tails = batch.tails;
    final int[] candidates = batch.candidates;
    while (numCandidates > 0) {
      for (int c = 0; c < numCandidates; c++) {
        final int j = candidates[c];
        final long position = recordPosition(partitions[j], slots[j]);
        heads[j] = in.readLong(position);
        if (keyLength > Integer.BYTES) {
          tails[j] = in.readLong(position + keyLength - Integer.BYTES);
        }
      }
      int numNext = 0;
      for (int c = 0; c < numCandidates; c++) {
        final int j = candidates[c];
        final int i = indices[j];
        final int offset = keys.offset(i);
        assert keys.length(i) == keyLength;
        final int doc =
            matchInline(
                in,
                recordPosition(partitions[j], slots[j]),
                heads[j],
                tails[j],
                bytes,
                offset,
                keyLength,
                inlinePrefix(bytes, offset, keyLength));
        if (doc != NO_MATCH) {
          docs[i - origin] = doc;
          continue;
        }
        final int slot = nextCandidate(in, partitions[j], homes[j], slots[j], keys.control(i));
        if (slot >= 0) {
          slots[j] = slot;
          candidates[numNext++] = j; // never after c, which is done with
        } else {
          docs[i - origin] = ABSENT;
        }
      }
      numCandidates = numNext;
    }
  }

  /**
   * The first slot after {@code slot} whose control byte is {@code control}, for a key of partition
   * {@code p} whose home slot is {@code home}, or -1 if an empty slot or the {@link #limit} comes
   * first.
   */
  private int nextCandidate(RandomAccessInput in, int p, int home, int slot, int control)
      throws IOException {
    final int next = slot + 1;
    final int limit = limit(p, home);
    if (next >= limit) {
      return -1;
    }
    final long controlOffset = controlOffsets[p];
    return findCandidate(
        in, controlOffset, next, in.readLong(controlOffset + next), limit, repeat(control));
  }

  /** Every byte set to 1, to repeat a byte across a long. */
  private static final long ONES = 0x0101010101010101L;

  /** The high bit of every byte. */
  private static final long HIGH_BITS = 0x8080808080808080L;

  /** A long whose 8 bytes are all the given control byte. Package-private for tests. */
  static long repeat(int control) {
    return (control & 0xFFL) * ONES;
  }

  /**
   * Sets the high bit of every zero byte of {@code x}, and clears every other bit. The borrow out
   * of a zero byte may set the high bit of a byte above it too, so only the lowest byte with its
   * high bit set is known to be zero.
   */
  private static long zeroBytes(long x) {
    return (x - ONES) & ~x & HIGH_BITS;
  }

  /**
   * Scans the control bytes of a partition from {@code slot} on, 8 at a time, and returns the first
   * slot in {@code [slot, limit)} whose control byte is the looked-up key's, or -1 if an empty slot
   * or {@code limit} comes first. Package-private for tests.
   *
   * @param controlOffset where the partition's control bytes start
   * @param controls the control bytes of slots {@code slot} to {@code slot + 7}, as read at {@code
   *     controlOffset + slot}
   * @param limit where the scan ends, at most the partition's number of slots: the 8 zero bytes
   *     after the last control byte let it read 8 control bytes from any slot before that
   * @param wants the looked-up key's control byte, {@link #repeat repeated} 8 times
   */
  static int findCandidate(
      RandomAccessInput in, long controlOffset, int slot, long controls, int limit, long wants)
      throws IOException {
    while (true) {
      // Byte k is the control byte of slot + k. The lowest byte flagged by each mask is exact, so
      // the lowest of either is the first slot that is empty or has the key's control byte.
      final long stops = zeroBytes(controls) | zeroBytes(controls ^ wants);
      final int k = Long.numberOfTrailingZeros(stops) >>> 3; // 8 if there is none
      final int index = slot + k;
      if (index >= limit) {
        return -1;
      }
      if (k < Long.BYTES) {
        // the byte itself says which of the two it is
        final int control = (int) (controls >>> (k << 3)) & 0xFF;
        assert control == TermHash.EMPTY_CONTROL || control == (int) (wants & 0xFF);
        return control == TermHash.EMPTY_CONTROL ? -1 : index;
      }
      slot = index;
      controls = in.readLong(controlOffset + slot);
    }
  }

  /**
   * The end of the slots of partition {@code p} that a key whose home slot is {@code home} can be
   * in: no key is more than the partition's largest displacement past its home slot.
   */
  int limit(int p, int home) {
    final int slotCount = slotCounts[p];
    final int maxProbe = maxProbes[p];
    return maxProbe < slotCount - home ? home + maxProbe + 1 : slotCount;
  }

  /** The partition of a key whose hash is {@code hash}. */
  int partition(long hash) {
    return TermHash.partition(hash, partitionBits);
  }

  /** The home slot, in its {@link #partition} {@code p}, of a key whose hash is {@code hash}. */
  int homeSlot(long hash, int p) {
    return TermHash.homeSlot(hash, partitionBits, capacities[p]);
  }

  /** Where the control bytes of partition {@code p} start in the slice. */
  long controlOffset(int p) {
    return controlOffsets[p];
  }

  /**
   * Looks up one key: returns the id of its only document, {@link #ABSENT} or {@link #MULTI}. On a
   * field whose keys are inline, the key must have their length.
   *
   * @param hash the key's hash
   */
  int lookup(RandomAccessInput in, BytesRef key, long hash) throws IOException {
    final int p = partition(hash);
    return lookup(in, key, hash, p, homeSlot(hash, p));
  }

  /**
   * As {@link #lookup(RandomAccessInput, BytesRef, long)}, given the key's partition {@code p} and
   * its home slot there, {@code home}.
   */
  int lookup(RandomAccessInput in, BytesRef key, long hash, int p, int home) throws IOException {
    if (recordStride != 0) {
      assert key.length == keyLength;
      return probeInline(
          in,
          p,
          home,
          home,
          TermHash.control(hash),
          key.bytes,
          key.offset,
          inlinePrefix(key.bytes, key.offset, keyLength));
    }
    return probe(
        in,
        p,
        home,
        home,
        TermHash.tag(hash),
        TermHash.control(hash),
        key.bytes,
        key.offset,
        key.length);
  }

  /** Position in the slice of the inline record of slot {@code slot} of partition {@code p}. */
  long recordPosition(int p, int slot) {
    return recordsOffsets[p] + (long) slot * recordStride;
  }

  /**
   * Looks up a key of a field whose keys are inline in partition {@code p}, from {@code slot} on,
   * at or after its home slot {@code home}, until an empty slot or the {@link #limit}.
   *
   * @param prefix the key's {@link #inlinePrefix}
   */
  int probeInline(
      RandomAccessInput in,
      int p,
      int home,
      int slot,
      int control,
      byte[] key,
      int offset,
      int prefix)
      throws IOException {
    assert slot >= home;
    final long controlOffset = controlOffsets[p];
    final int limit = limit(p, home);
    final long wants = repeat(control);
    while (slot < limit) {
      slot =
          findCandidate(in, controlOffset, slot, in.readLong(controlOffset + slot), limit, wants);
      if (slot < 0) {
        return ABSENT;
      }
      final int doc = matchInline(in, recordPosition(p, slot), key, offset, prefix);
      if (doc != NO_MATCH) {
        return doc;
      }
      slot++;
    }
    return ABSENT;
  }

  /**
   * Reads the head and, if the field's keys are longer than 4 bytes, the tail of the inline record
   * at {@code position}, both before comparing either, then compares the record's key with the
   * given key, of the field's key length, with {@link #matchInline(RandomAccessInput, long, long,
   * long, byte[], int, int, int)}.
   *
   * @param prefix the key's {@link #inlinePrefix}
   */
  private int matchInline(RandomAccessInput in, long position, byte[] key, int offset, int prefix)
      throws IOException {
    final long head = in.readLong(position);
    final long tail =
        keyLength > Integer.BYTES ? in.readLong(position + keyLength - Integer.BYTES) : 0;
    return matchInline(in, position, head, tail, key, offset, keyLength, prefix);
  }

  /**
   * The first bytes of a key of {@code length} bytes, as they are in the high half of the head of
   * its inline record read as a little-endian long: its first 4 bytes, or all of them, followed by
   * zeros, if it is shorter.
   */
  private static int inlinePrefix(byte[] key, int offset, int length) {
    if (length >= Integer.BYTES) {
      return (int) BitUtil.VH_LE_INT.get(key, offset);
    }
    int prefix = 0;
    for (int i = 0; i < length; i++) {
      prefix |= (key[offset + i] & 0xFF) << (Byte.SIZE * i);
    }
    return prefix;
  }

  /**
   * Compares the key of the inline record at {@code position} with the given key, whose {@link
   * #inlinePrefix} is {@code prefix}, and which has {@code length} bytes, as the record's key does.
   * {@code head} is the record's first 8 bytes and, if the keys are longer than 4 bytes, {@code
   * tail} is its last 8: together they cover both cache lines of a record that crosses one. The
   * key's first bytes are compared with the head, leaving out the bytes after a key shorter than 4
   * bytes, and the rest of a longer key with {@link #restEquals}. Returns the record's document,
   * {@link #MULTI}, or {@link #NO_MATCH}.
   */
  private static int matchInline(
      RandomAccessInput in,
      long position,
      long head,
      long tail,
      byte[] key,
      int offset,
      int length,
      int prefix)
      throws IOException {
    final int mask = length >= Integer.BYTES ? -1 : (1 << (Byte.SIZE * length)) - 1;
    if (((int) (head >>> 32) & mask) != prefix
        || (length > Integer.BYTES
            && restEquals(in, position, tail, key, offset, length) == false)) {
      return NO_MATCH;
    }
    return docOf(head);
  }

  /**
   * The document of a record whose first 8 bytes are {@code head}, or {@link #MULTI}. Inline
   * records and records in term order both start with the document id.
   */
  private static int docOf(long head) {
    final int doc = (int) head;
    return doc >= 0 ? doc : MULTI;
  }

  /**
   * Compares the key of the inline record at {@code position}, whose last 8 bytes are {@code tail},
   * with {@code key}, of {@code length > 4} bytes, from its fifth byte on: the first 4 are in the
   * record's head, which was compared already. The key's last 8 bytes, or last 4 if it is shorter
   * than 8, are compared with the tail, and the bytes between the first 4 and the last 8 by machine
   * words, the last one overlapping the tail, rather than masked. Package-private for tests.
   */
  static boolean restEquals(
      RandomAccessInput in, long position, long tail, byte[] key, int offset, int length)
      throws IOException {
    assert length > Integer.BYTES;
    if (length < Long.BYTES) {
      return (int) (tail >>> 32)
          == (int) BitUtil.VH_LE_INT.get(key, offset + length - Integer.BYTES);
    }
    if (tail != (long) BitUtil.VH_LE_LONG.get(key, offset + length - Long.BYTES)) {
      return false;
    }
    final long keyPosition = position + Integer.BYTES;
    final int last = length - Long.BYTES;
    for (int i = Integer.BYTES; i < last; i += Long.BYTES) {
      if (in.readLong(keyPosition + i) != (long) BitUtil.VH_LE_LONG.get(key, offset + i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Looks a key up in partition {@code p} from {@code slot} on, where {@code home} is the key's
   * home slot, until an empty slot or the {@link #limit}.
   */
  int probe(
      RandomAccessInput in,
      int p,
      int home,
      int slot,
      int tag,
      int control,
      byte[] key,
      int offset,
      int length)
      throws IOException {
    final long controlOffset = controlOffsets[p];
    final int limit = limit(p, home);
    final long wants = repeat(control);
    while (slot < limit) {
      slot =
          findCandidate(in, controlOffset, slot, in.readLong(controlOffset + slot), limit, wants);
      if (slot < 0) {
        return ABSENT;
      }
      final int doc = verify(in, p, slot, tag, key, offset, length);
      if (doc != NO_MATCH) {
        return doc;
      }
      slot++;
    }
    return ABSENT;
  }

  /**
   * Verifies a slot of partition {@code p} whose control byte is the looked-up key's: returns the
   * key's document, or {@link #MULTI}, if the slot holds the key; else {@link #NO_MATCH}, and the
   * scan goes on.
   */
  private int verify(
      RandomAccessInput in, int p, int slot, int tag, byte[] key, int offset, int length)
      throws IOException {
    final long word = in.readLong(slotsOffsets[p] + ((long) slot << 3));
    if ((word & TAG_MASK) != tag) {
      return NO_MATCH;
    }
    final long position = recordPosition(word);
    return matchRecord(in, position, in.readLong(position), key, offset, length);
  }

  /** The position in the slice of the record a non-empty slot points to. */
  static long recordPosition(long word) {
    return (word >>> POINTER_SHIFT) - 1;
  }

  /**
   * Compares the key of the record at {@code position}, whose first 8 bytes are {@code head}, with
   * the given key. Returns the record's document, {@link #MULTI}, or {@link #NO_MATCH}.
   *
   * <p>The first 8 bytes of a record hold its 4-byte doc id and its VInt key length, which takes
   * one byte for keys shorter than 128 bytes and at most three for any term Lucene accepts, so one
   * read usually covers both. A record is at least 5 bytes long and is followed by at least 16
   * control bytes, so that read never leaves the slice.
   */
  static int matchRecord(
      RandomAccessInput in, long position, long head, byte[] key, int offset, int length)
      throws IOException {
    int storedLength;
    int headerLength;
    if ((head & (0x80L << 32)) == 0) {
      storedLength = (int) (head >>> 32) & 0x7F;
      headerLength = Integer.BYTES + 1;
    } else {
      storedLength = 0;
      int i = 0;
      while (true) {
        int b =
            i < Integer.BYTES
                ? (int) (head >>> (32 + 8 * i)) & 0xFF
                : in.readByte(position + Integer.BYTES + i) & 0xFF;
        storedLength |= (b & 0x7F) << (7 * i);
        if ((b & 0x80) == 0) {
          break;
        }
        if (++i == 5) {
          return NO_MATCH; // not a valid VInt
        }
      }
      headerLength = Integer.BYTES + i + 1;
    }
    if (storedLength != length
        || keyEquals(in, position + headerLength, key, offset, length) == false) {
      return NO_MATCH;
    }
    return docOf(head);
  }

  /**
   * Compares {@code length} bytes at {@code position} with {@code key} in place, a machine word at
   * a time, reading the last word so that it overlaps the previous one rather than masking it.
   * Package-private for tests.
   */
  static boolean keyEquals(RandomAccessInput in, long position, byte[] key, int offset, int length)
      throws IOException {
    if (length >= Long.BYTES) {
      final int last = length - Long.BYTES;
      for (int i = 0; i < last; i += Long.BYTES) {
        if (in.readLong(position + i) != (long) BitUtil.VH_LE_LONG.get(key, offset + i)) {
          return false;
        }
      }
      return in.readLong(position + last) == (long) BitUtil.VH_LE_LONG.get(key, offset + last);
    }
    if (length >= Integer.BYTES) {
      final int last = length - Integer.BYTES;
      return in.readInt(position) == (int) BitUtil.VH_LE_INT.get(key, offset)
          && in.readInt(position + last) == (int) BitUtil.VH_LE_INT.get(key, offset + last);
    }
    for (int i = 0; i < length; i++) {
      if (in.readByte(position + i) != key[offset + i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(partitions="
        + controlOffsets.length
        + (keyLength >= 0 ? ", inline keys of " + keyLength + " bytes" : "")
        + ", filterWords="
        + filterWords
        + ", delegate="
        + delegate
        + ")";
  }
}
