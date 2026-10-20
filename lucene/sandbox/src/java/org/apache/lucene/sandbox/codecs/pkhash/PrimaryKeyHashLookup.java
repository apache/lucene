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

import static org.apache.lucene.sandbox.codecs.pkhash.HashTerms.ABSENT;
import static org.apache.lucene.sandbox.codecs.pkhash.HashTerms.MULTI;
import static org.apache.lucene.sandbox.codecs.pkhash.HashTerms.NO_MATCH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Finds a live document of an {@link IndexReader} whose {@code field} contains a given term, called
 * a key here, by looking the key up in the hash tables that {@link PrimaryKeyHashPostingsFormat}
 * wrote for all the segments of the reader at once, rather than in one segment after the other.
 * Resolving a primary key or another unique id to its document is the typical use.
 *
 * <p>The result is that of the test framework's {@code PerThreadPKLookup}: segments are tried in
 * decreasing order of their number of live documents ({@link IndexReader#numDocs()}), segments with
 * as many in the order of {@link IndexReader#leaves()}, the documents of a segment in increasing
 * order, and the first live document whose {@code field} contains the key is returned, as a
 * document id of the reader (its segment's {@code docBase} plus its id in the segment), or -1 if
 * there is none. When no two live documents share a key, as on a primary-key field, this is the
 * only live document with the key; otherwise it is one of them. A key whose documents in a segment
 * are all deleted is looked for in the next segments.
 *
 * <p>Segments whose {@link Terms} for the field is a {@link HashTerms} are looked up in their hash
 * tables together. The key is hashed, and its first 8 bytes read, once for all of them. Segments
 * whose smallest term sorts after the key, or whose largest term sorts before it, and segments
 * whose keys are inline and have another length than the key, are left out without any read. The
 * others are looked up in the stages of {@link HashTerms#lookupBatch}: the filter, in segments that
 * have one, then the control bytes, the slots and the records. Each stage makes its reads in all
 * the segments that are still looking before it uses any of them, so that their cache misses
 * overlap instead of being paid one after the other. If the range checks leave a single pair of a
 * key and a segment, the filter is skipped, since its read would have no other read to overlap
 * with, and the key is looked up in that segment as {@link TermsEnum#seekExact(BytesRef)} does.
 *
 * <p>The segments are then gone through in their order, with what the tables found. A key with
 * several documents in a segment (see {@link HashTerms#MULTI}) is sought in that segment's terms
 * dictionary when its turn comes, and its postings are read up to the first live document. Segments
 * without a table (another postings format, a field that has no table, or a reader wrapper that
 * hides the {@link HashTerms}) are sought with {@link TermsEnum#seekExact(BytesRef)} in their turn,
 * only if no earlier segment had a live document with the key.
 *
 * <p>{@link #lookup(BytesRef[], int, int, int[])} looks up many keys in chunks, every stage reading
 * for all the keys of a chunk in all segments. Chunks hold more keys when a segment has a filter: a
 * unique key is in at most one segment, so most pairs of a key and a segment stop at the filter.
 * Unlike {@link HashTerms#lookupBatch}, it takes keys in any order, does not sort them, and does
 * not remove duplicates.
 *
 * <p>An instance is bound to the reader it was created with, whose segments, live documents and
 * enums it keeps: create a new one for every new reader. It is not thread-safe: use one per thread.
 * Creating one is cheap, about a clone of an input per segment with a table and a few arrays with
 * an entry per segment. Lookups allocate nothing in steady state: the terms and postings enums of a
 * segment, and the scratch space for one key or for a chunk, are allocated on first use.
 *
 * @lucene.experimental
 */
public final class PrimaryKeyHashLookup {

  /**
   * The most keys of a chunk of {@link #lookup(BytesRef[], int, int, int[])} when no segment has a
   * filter.
   */
  static final int MAX_CHUNK_KEYS = HashTerms.CHUNK_SIZE;

  /**
   * The most pairs of a key and a segment with a table that a chunk of {@link #lookup(BytesRef[],
   * int, int, int[])} looks up together, unless there are more segments with a table than this: a
   * chunk then has a single key. This bounds the scratch space.
   */
  static final int MAX_CHUNK_ENTRIES = 256;

  private final String field;

  // Every segment that has live documents and terms of the field, in the order they are tried in.
  private final int numLeaves;
  private final int[] docBases;
  private final Bits[] liveDocs;

  /** For every segment, the index of its table in {@link #tables}, or -1 if it has none. */
  private final int[] tableOf;

  /**
   * For every segment, the terms that keys are sought in with a {@link TermsEnum}: those of the
   * delegate format, for a segment with a table, which is only for keys with several documents.
   */
  private final Terms[] seekTerms;

  private final TermsEnum[] termsEnums; // created on first use
  private final PostingsEnum[] postingsEnums;

  // The segments with a table, in the same order.
  private final int numTables;
  private final HashTerms[] tables;
  private final RandomAccessInput[] inputs;
  private final boolean anyFilter;

  /** The number of keys of a chunk of {@link #lookup(BytesRef[], int, int, int[])}. */
  private final int chunkKeys;

  private Scratch scratch; // for one key, or a whole chunk, allocated by the first lookup of them
  private final BytesRef[] singleKey = new BytesRef[1];

  /**
   * Creates a lookup of the keys of {@code field} in the live documents of {@code reader}.
   *
   * @throws IOException if the terms of a segment cannot be read
   */
  public PrimaryKeyHashLookup(IndexReader reader, String field) throws IOException {
    Objects.requireNonNull(reader, "reader must not be null");
    this.field = Objects.requireNonNull(field, "field must not be null");
    final List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());
    // Larger segments are more likely to hold the key, as in PerThreadPKLookup. The sort is
    // stable, so segments with as many live documents stay in the order of the reader's leaves.
    leaves.sort(
        Comparator.comparingInt((LeafReaderContext context) -> context.reader().numDocs())
            .reversed());
    final int maxLeaves = leaves.size();
    docBases = new int[maxLeaves];
    liveDocs = new Bits[maxLeaves];
    tableOf = new int[maxLeaves];
    seekTerms = new Terms[maxLeaves];
    termsEnums = new TermsEnum[maxLeaves];
    postingsEnums = new PostingsEnum[maxLeaves];
    tables = new HashTerms[maxLeaves];
    inputs = new RandomAccessInput[maxLeaves];
    int numLeaves = 0;
    int numTables = 0;
    boolean anyFilter = false;
    for (LeafReaderContext context : leaves) {
      final LeafReader leaf = context.reader();
      if (leaf.numDocs() == 0) {
        continue; // nothing to find there
      }
      final Terms terms = leaf.terms(field);
      if (terms == null) {
        continue;
      }
      docBases[numLeaves] = context.docBase;
      liveDocs[numLeaves] = leaf.getLiveDocs();
      if (terms instanceof HashTerms table) {
        tableOf[numLeaves] = numTables;
        seekTerms[numLeaves] = table.getDelegate();
        tables[numTables] = table;
        inputs[numTables] = table.newSlice();
        anyFilter |= table.hasFilter();
        numTables++;
      } else {
        tableOf[numLeaves] = -1;
        seekTerms[numLeaves] = terms;
      }
      numLeaves++;
    }
    this.numLeaves = numLeaves;
    this.numTables = numTables;
    this.anyFilter = anyFilter;
    // With a filter, most entries of a chunk do not get past it, so chunks get as many keys as
    // the scratch space holds: the stages after the filters then have about as many entries to
    // overlap as without one.
    if (numTables == 0) {
      this.chunkKeys = MAX_CHUNK_KEYS;
    } else if (anyFilter) {
      this.chunkKeys = Math.max(1, MAX_CHUNK_ENTRIES / numTables);
    } else {
      this.chunkKeys = Math.max(1, Math.min(MAX_CHUNK_KEYS, MAX_CHUNK_ENTRIES / numTables));
    }
  }

  /** The number of segments that are looked in. For tests. */
  int numLeaves() {
    return numLeaves;
  }

  /** The number of segments that are looked in through their hash table. For tests. */
  int numTables() {
    return numTables;
  }

  /** The number of keys of a chunk of {@link #lookup(BytesRef[], int, int, int[])}. For tests. */
  int chunkKeys() {
    return chunkKeys;
  }

  /** The number of keys that the scratch space holds, 0 before it is allocated. For tests. */
  int scratchKeys() {
    return scratch == null ? 0 : scratch.numKeys;
  }

  /**
   * Returns the document id, in the reader, of the first live document whose field contains {@code
   * key}, in the order described {@link PrimaryKeyHashLookup above}, or -1 if there is none.
   *
   * @throws IOException if the index cannot be read
   */
  public int lookup(BytesRef key) throws IOException {
    Objects.requireNonNull(key, "key must not be null");
    if (numTables == 0) {
      return firstLive(key, null, 0);
    }
    final Scratch scratch = scratch(1);
    singleKey[0] = key;
    lookupInTables(singleKey, 0, 1, scratch);
    singleKey[0] = null;
    return firstLive(key, scratch.results, 0);
  }

  /**
   * Looks up the keys {@code [from, to)} of {@code keys} and writes, for each key {@code i}, to
   * {@code docs[i - from]} what {@link #lookup(BytesRef)} returns for it. Keys may be in any order
   * and may repeat. The keys are looked up in chunks, all keys of a chunk in all segments together.
   *
   * @throws IndexOutOfBoundsException if {@code [from, to)} is not a range of {@code keys}
   * @throws IllegalArgumentException if {@code docs} is shorter than {@code to - from}
   * @throws NullPointerException if a key in the range is null; nothing is written then
   * @throws IOException if the index cannot be read
   */
  public void lookup(BytesRef[] keys, int from, int to, int[] docs) throws IOException {
    Objects.checkFromToIndex(from, to, keys.length);
    if (docs.length < to - from) {
      throw new IllegalArgumentException(
          "docs has length " + docs.length + " but " + (to - from) + " keys were requested");
    }
    for (int i = from; i < to; i++) {
      Objects.requireNonNull(keys[i], "keys must not contain null");
    }
    if (from == to) {
      return;
    }
    if (numTables == 0) {
      for (int i = from; i < to; i++) {
        docs[i - from] = firstLive(keys[i], null, 0);
      }
      return;
    }
    // for a whole chunk, so that batches of other sizes do not allocate again
    final Scratch scratch = scratch(chunkKeys);
    for (int base = from; base < to; base += chunkKeys) {
      final int n = Math.min(chunkKeys, to - base);
      lookupInTables(keys, base, n, scratch);
      for (int k = 0; k < n; k++) {
        docs[base + k - from] = firstLive(keys[base + k], scratch.results, k * numTables);
      }
    }
  }

  /** Scratch space for {@code numKeys} keys, at least. */
  private Scratch scratch(int numKeys) {
    if (scratch == null || scratch.numKeys < numKeys) {
      scratch = new Scratch(numKeys, numTables, anyFilter);
    }
    return scratch;
  }

  /**
   * Goes through the segments in their order and returns the document id, in the reader, of the
   * first live document that contains the key, or -1. The result of the key in the table {@code t}
   * is at {@code results[first + t]}: a document whose liveness is checked here, {@link
   * HashTerms#ABSENT}, or {@link HashTerms#MULTI}, which is sought in the segment's terms
   * dictionary, as are keys in segments without a table.
   */
  private int firstLive(BytesRef key, int[] results, int first) throws IOException {
    for (int i = 0; i < numLeaves; i++) {
      final int t = tableOf[i];
      if (t >= 0) {
        final int doc = results[first + t];
        if (doc >= 0) {
          final Bits live = liveDocs[i];
          if (live == null || live.get(doc)) {
            return docBases[i] + doc;
          }
          continue; // the key's only document in the segment is deleted
        } else if (doc == ABSENT) {
          continue;
        }
        assert doc == MULTI : doc;
      }
      final int doc = seekFirstLive(i, key);
      if (doc != DocIdSetIterator.NO_MORE_DOCS) {
        return docBases[i] + doc;
      }
    }
    return -1;
  }

  /**
   * Seeks the key in the terms dictionary of segment {@code i} and returns its first live document
   * in the segment, or {@link DocIdSetIterator#NO_MORE_DOCS} if it has none.
   */
  private int seekFirstLive(int i, BytesRef key) throws IOException {
    TermsEnum termsEnum = termsEnums[i];
    if (termsEnum == null) {
      termsEnum = termsEnums[i] = seekTerms[i].iterator();
    }
    if (termsEnum.seekExact(key) == false) {
      return DocIdSetIterator.NO_MORE_DOCS;
    }
    final PostingsEnum postings =
        postingsEnums[i] = termsEnum.postings(postingsEnums[i], PostingsEnum.NONE);
    final Bits live = liveDocs[i];
    for (int doc = postings.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = postings.nextDoc()) {
      if (live == null || live.get(doc)) {
        return doc;
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Looks the keys {@code [base, base + n)} up in every table, in stages, and writes the result of
   * key {@code base + k} in table {@code t} to {@code scratch.results[k * numTables + t]}: the id
   * of its only document in the segment, deleted or not, {@link HashTerms#ABSENT} or {@link
   * HashTerms#MULTI}.
   *
   * <p>An entry is a pair of a key and a table whose range of terms holds it, and whose keys, if
   * they are inline, have the key's length. Each stage goes through the entries still looking and
   * reads for each of them, at positions that only depend on what earlier stages read, and nothing
   * branches on what a stage reads until all of its first reads are made: a word of the filter, the
   * control bytes from a home slot, a slot word or the first and last 8 bytes of an inline record,
   * the head of a record. These are the stages of {@link HashTerms#lookupBatch}, over the entries
   * of many tables rather than the keys of one. Within a table, an entry reads its key's word of
   * the filter, if the table has one, and then, unless the filter rules the key out, what {@link
   * TermsEnum#seekExact(BytesRef)} reads for the key, in the same order. A single entry does not
   * read the filter, and reads exactly what {@code seekExact} reads. The entries that the filters
   * let through are moved to the front, and every later stage goes through them in order, with what
   * an entry needs in arrays of its own.
   *
   * <p>In a reader where some segments have a filter and others do not, the control bytes of the
   * segments without one are read after the filters of the others, together with those of the
   * entries that the filters let through.
   */
  private void lookupInTables(BytesRef[] keys, int base, int n, Scratch scratch)
      throws IOException {
    final int numTables = this.numTables;
    final HashTerms[] tables = this.tables;
    final RandomAccessInput[] inputs = this.inputs;
    final int[] entryKeys = scratch.entryKeys;
    final int[] entryTables = scratch.entryTables;
    final long[] entryHashes = scratch.entryHashes;

    // Range check: an entry for every pair of a key and a table whose range of terms holds it. A
    // key is hashed and mixed once for all its entries, and not at all if it has none. With the
    // partition bits of each table, the mix gives the key's word in the filter of every entry
    // whose table has one, and the key's bits in it are the same in every filter. An entry whose
    // table has no filter gets no bits, which every word has.
    final long[] filterMasks = scratch.filterMasks;
    final long[] filterPositions = scratch.filterPositions;
    Arrays.fill(scratch.results, 0, n * numTables, ABSENT);
    int numEntries = 0;
    for (int k = 0; k < n; k++) {
      final BytesRef key = keys[base + k];
      final byte[] bytes = key.bytes;
      final int offset = key.offset;
      final int length = key.length;
      final long prefix = HashTerms.prefix(bytes, offset, length);
      final int firstEntry = numEntries;
      for (int t = 0; t < numTables; t++) {
        if (tables[t].outOfRange(prefix, bytes, offset, length) == false
            && tables[t].excludesLength(length) == false) {
          entryKeys[numEntries] = k;
          entryTables[numEntries++] = t;
        }
      }
      if (numEntries > firstEntry) {
        final long hash = TermHash.hash(bytes, offset, length);
        for (int e = firstEntry; e < numEntries; e++) {
          entryHashes[e] = hash;
        }
        if (anyFilter) {
          final long mixed = TermHash.mix(hash);
          final long mask = BlockedBloomFilter.mask(mixed);
          for (int e = firstEntry; e < numEntries; e++) {
            final HashTerms table = tables[entryTables[e]];
            if (table.hasFilter()) {
              filterMasks[e] = mask;
              filterPositions[e] = table.filterWordPosition(table.filterHash(mixed, hash));
            } else {
              filterMasks[e] = 0;
            }
          }
        }
      }
    }

    // Stage 0: the filters, which leave the entries that they let through at the front. A single
    // entry has no other reads to overlap the read of its filter with: as seekExact does, it goes
    // straight to the control bytes.
    if (anyFilter && numEntries > 1) {
      numEntries = applyFilters(scratch, numEntries);
    }

    // Stage 1: the control bytes of the home slot of every entry, and of the 7 slots after, and
    // nothing else. Where they are is found first, with what their scan needs.
    final int[] partitions = scratch.partitions;
    final int[] homes = scratch.homes;
    final int[] limits = scratch.limits;
    final long[] controlOffsets = scratch.controlOffsets;
    final long[] controlPositions = scratch.controlPositions;
    final long[] controls = scratch.controls;
    for (int e = 0; e < numEntries; e++) {
      final HashTerms table = tables[entryTables[e]];
      final long hash = entryHashes[e];
      final int p = table.partition(hash);
      final int home = table.homeSlot(hash, p);
      final long controlOffset = table.controlOffset(p);
      partitions[e] = p;
      homes[e] = home;
      limits[e] = table.limit(p, home);
      controlOffsets[e] = controlOffset;
      controlPositions[e] = controlOffset + home;
    }
    for (int e = 0; e < numEntries; e++) {
      controls[e] = inputs[entryTables[e]].readLong(controlPositions[e]);
    }

    // Stage 2: scan the control bytes of every entry to the first one that is its key's. Most
    // misses end here; more control bytes are only read by scans past 8 slots.
    final int[] slots = scratch.slots;
    final int[] candidates = scratch.candidates;
    int numCandidates = 0;
    for (int e = 0; e < numEntries; e++) {
      final int slot =
          HashTerms.findCandidate(
              inputs[entryTables[e]],
              controlOffsets[e],
              homes[e],
              controls[e],
              limits[e],
              HashTerms.repeat(TermHash.control(entryHashes[e])));
      if (slot >= 0) {
        slots[e] = slot;
        candidates[numCandidates++] = e;
      }
    }

    // Stages 3 and 4, in rounds until no entry has a candidate left. Stage 3: the slot of every
    // candidate, or for inline keys the first 8 bytes of its record and, for keys of more than 4
    // bytes, its last 8. Stage 4: check tags, and read the head of the record of every candidate
    // whose tag is its key's, or for inline keys compare the key with the record,
    // while an entry whose candidate holds another key scans on to its next candidate, which is
    // read in the next round.
    final long[] words = scratch.words;
    final long[] tails = scratch.tails;
    final long[] heads = scratch.heads;
    final int[] pending = scratch.pending;
    final int[] results = scratch.results;
    int numPending = 0;
    while (numCandidates > 0) {
      for (int c = 0; c < numCandidates; c++) {
        final int e = candidates[c];
        final HashTerms table = tables[entryTables[e]];
        final RandomAccessInput in = inputs[entryTables[e]];
        final long position = table.candidatePosition(partitions[e], slots[e]);
        words[e] = in.readLong(position);
        final int keyLength = table.keyLength(); // -1 if the records are in term order
        if (keyLength > Integer.BYTES) {
          tails[e] = in.readLong(position + keyLength - Integer.BYTES);
        }
      }
      int numNext = 0;
      for (int c = 0; c < numCandidates; c++) {
        final int e = candidates[c];
        final int t = entryTables[e];
        final HashTerms table = tables[t];
        final long hash = entryHashes[e];
        if (table.isInline()) {
          final BytesRef key = keys[base + entryKeys[e]];
          final int doc =
              HashTerms.matchInline(
                  inputs[t],
                  table.recordPosition(partitions[e], slots[e]),
                  words[e],
                  tails[e],
                  key.bytes,
                  key.offset,
                  key.length,
                  HashTerms.inlinePrefix(key.bytes, key.offset, key.length));
          if (doc != NO_MATCH) {
            results[entryKeys[e] * numTables + t] = doc;
            continue;
          }
        } else {
          final long word = words[e];
          if ((word & TermHash.TAG_MASK) == TermHash.tag(hash)) {
            heads[e] = inputs[t].readLong(HashTerms.recordPosition(word));
            pending[numPending++] = e;
            continue;
          }
        }
        final int next = slots[e] + 1;
        final int limit = limits[e];
        if (next < limit) {
          final long controlOffset = controlOffsets[e];
          final int slot =
              HashTerms.findCandidate(
                  inputs[t],
                  controlOffset,
                  next,
                  inputs[t].readLong(controlOffset + next),
                  limit,
                  HashTerms.repeat(TermHash.control(hash)));
          if (slot >= 0) {
            slots[e] = slot;
            candidates[numNext++] = e; // never after c, which is done with
          }
        }
      }
      numCandidates = numNext;
    }

    // Stage 5: compare keys with the records whose tag is their own, in tables whose records are
    // in term order. A tag also matches another key's once per 2^16 or so; the scan then resumes
    // after that slot, one entry at a time, verifying every match.
    for (int c = 0; c < numPending; c++) {
      final int e = pending[c];
      final int k = entryKeys[e];
      final int t = entryTables[e];
      final BytesRef key = keys[base + k];
      int doc =
          HashTerms.matchRecord(
              inputs[t],
              HashTerms.recordPosition(words[e]),
              heads[e],
              key.bytes,
              key.offset,
              key.length);
      if (doc == NO_MATCH) {
        final long hash = entryHashes[e];
        doc =
            tables[t].probe(
                inputs[t],
                partitions[e],
                homes[e],
                slots[e] + 1,
                TermHash.tag(hash),
                TermHash.control(hash),
                key.bytes,
                key.offset,
                key.length);
      }
      results[k * numTables + t] = doc;
    }
  }

  /**
   * Stage 0 of {@link #lookupInTables}: reads the key's word of the filter of every entry whose
   * table has one, at the position found with the entries, and moves the entries that the filters
   * do not rule out, in order, to the front. Returns how many there are. As in {@link
   * HashTerms#lookupBatch}, the words are read in a loop that does nothing else, and which entries
   * pass is only looked at once every word is read, by a loop that does not branch on it: every
   * entry is copied to the front, and only those that pass stay there. An entry whose table has no
   * filter passes, whatever its word holds, since it has no bits.
   */
  private int applyFilters(Scratch scratch, int numEntries) throws IOException {
    final HashTerms[] tables = this.tables;
    final RandomAccessInput[] inputs = this.inputs;
    final int[] entryKeys = scratch.entryKeys;
    final int[] entryTables = scratch.entryTables;
    final long[] entryHashes = scratch.entryHashes;
    final long[] masks = scratch.filterMasks;
    final long[] positions = scratch.filterPositions;
    final long[] words = scratch.filterWords;
    for (int e = 0; e < numEntries; e++) {
      final int t = entryTables[e];
      if (tables[t].hasFilter()) {
        words[e] = inputs[t].readLong(positions[e]);
      }
    }
    int numKept = 0;
    for (int e = 0; e < numEntries; e++) {
      // numKept <= e, so this only overwrites entries that were gone through
      entryKeys[numKept] = entryKeys[e];
      entryTables[numKept] = entryTables[e];
      entryHashes[numKept] = entryHashes[e];
      numKept += BlockedBloomFilter.mayContain(words[e], masks[e]);
    }
    return numKept;
  }

  /**
   * Scratch space of the lookups in the tables: per pair of a key and a table (results), and per
   * entry, of which there are at most as many.
   */
  private static final class Scratch {
    final int numKeys;
    final int[] results;
    final int[] entryKeys;
    final int[] entryTables;
    final long[] entryHashes;
    final long[] filterMasks; // these three are null if no table has a filter
    final long[] filterPositions;
    final long[] filterWords;
    final int[] partitions;
    final int[] homes;
    final int[] limits;
    final long[] controlOffsets;
    final long[] controlPositions;
    final long[] controls;
    final int[] slots;
    final int[] candidates;
    final long[] words; // slot words, or the first 8 bytes of inline records
    final long[] tails; // the last 8 bytes of inline records
    final long[] heads; // the first 8 bytes of records in term order
    final int[] pending;

    Scratch(int numKeys, int numTables, boolean filter) {
      this.numKeys = numKeys;
      final int entries = Math.multiplyExact(numKeys, numTables);
      results = new int[entries];
      entryKeys = new int[entries];
      entryTables = new int[entries];
      entryHashes = new long[entries];
      filterMasks = filter ? new long[entries] : null;
      filterPositions = filter ? new long[entries] : null;
      filterWords = filter ? new long[entries] : null;
      partitions = new int[entries];
      homes = new int[entries];
      limits = new int[entries];
      controlOffsets = new long[entries];
      controlPositions = new long[entries];
      controls = new long[entries];
      slots = new int[entries];
      candidates = new int[entries];
      words = new long[entries];
      tails = new long[entries];
      heads = new long[entries];
      pending = new int[entries];
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(field="
        + field
        + ", segments="
        + numLeaves
        + ", withTable="
        + numTables
        + ")";
  }
}
