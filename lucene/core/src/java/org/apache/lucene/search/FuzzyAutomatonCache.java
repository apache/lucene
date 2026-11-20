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
package org.apache.lucene.search;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A bounded, thread-safe LRU cache of the Levenshtein automata that {@link FuzzyQuery} builds to
 * match terms.
 *
 * <p>Building these automata is expensive, and without a cache they are rebuilt every time a {@link
 * FuzzyQuery} is rewritten. Pass one shared instance to {@link
 * FuzzyQuery#FuzzyQuery(org.apache.lucene.index.Term, int, int, int, boolean,
 * MultiTermQuery.RewriteMethod, FuzzyAutomatonCache)} to reuse them across queries, fields and
 * requests.
 *
 * <p>Design notes:
 *
 * <ul>
 *   <li><b>The field is not part of the key.</b> An automaton depends only on the term text, the
 *       maximum number of edits, the prefix length and whether transpositions are allowed. Entries
 *       are keyed on exactly those four values, so the same token searched on several fields builds
 *       its automata once.
 *   <li><b>The cache is opt-in and owned by the application.</b> There is no static or global
 *       instance: the application decides the bounds and the lifetime, and queries built without a
 *       cache behave exactly as before.
 *   <li><b>Queries stay small.</b> A {@link FuzzyQuery} only holds a reference to the cache, never
 *       to the automata, and the cache does not take part in {@link FuzzyQuery#equals(Object)} or
 *       {@link FuzzyQuery#hashCode()}. Caches keyed on queries are therefore unaffected.
 *   <li><b>Entries are independent of the index.</b> Nothing has to be invalidated when readers are
 *       reopened or closed.
 * </ul>
 *
 * <p>The cache is bounded both by number of entries and by estimated heap usage; the least recently
 * used entries are evicted first. Automata are built outside of the internal lock, so two threads
 * that miss on the same key at the same time may both build it; one result is kept.
 *
 * @lucene.experimental
 */
public final class FuzzyAutomatonCache implements Accountable {

  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(FuzzyAutomatonCache.class);
  private static final long ENTRY_OVERHEAD_RAM_BYTES =
      RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY
          + RamUsageEstimator.shallowSizeOfInstance(Key.class)
          + RamUsageEstimator.shallowSizeOfInstance(Entry.class)
          + RamUsageEstimator.shallowSizeOfInstance(Automata.class);

  /** The automata for edit distances {@code 0..maxEdits} of one term, shared between threads. */
  record Automata(CompiledAutomaton[] automata, int termLength) {}

  private record Key(String term, int maxEdits, int prefixLength, boolean transpositions) {}

  private record Entry(Automata automata, long ramBytesUsed) {}

  private final int maxSize;
  private final long maxRamBytesUsed;

  // access-ordered: iteration starts at the least recently used entry
  private final LinkedHashMap<Key, Entry> cache = new LinkedHashMap<>(16, 0.75f, true);
  private long ramBytesUsed; // guarded by "this"
  private long evictionCount; // guarded by "this"

  private final LongAdder hitCount = new LongAdder();
  private final LongAdder missCount = new LongAdder();

  /**
   * Creates a new cache.
   *
   * @param maxSize the maximum number of distinct (term, maxEdits, prefixLength, transpositions)
   *     combinations to keep
   * @param maxRamBytesUsed the maximum estimated heap usage of the cached automata, in bytes
   */
  public FuzzyAutomatonCache(int maxSize, long maxRamBytesUsed) {
    if (maxSize < 0) {
      throw new IllegalArgumentException("maxSize must be >= 0; got: " + maxSize);
    }
    if (maxRamBytesUsed < 0) {
      throw new IllegalArgumentException("maxRamBytesUsed must be >= 0; got: " + maxRamBytesUsed);
    }
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
  }

  /** Builds the automata without consulting any cache. */
  static Automata build(String term, int maxEdits, int prefixLength, boolean transpositions) {
    FuzzyAutomatonBuilder builder =
        new FuzzyAutomatonBuilder(term, maxEdits, prefixLength, transpositions);
    return new Automata(builder.buildAutomatonSet(), builder.getTermLength());
  }

  /**
   * Returns the automata for the given parameters, building and caching them if they are absent.
   */
  Automata getOrBuild(String term, int maxEdits, int prefixLength, boolean transpositions) {
    // A prefix longer than the term behaves like a prefix of the whole term: share the entry.
    int normalizedPrefixLength = Math.min(prefixLength, term.codePointCount(0, term.length()));
    Key key = new Key(term, maxEdits, normalizedPrefixLength, transpositions);

    synchronized (this) {
      Entry entry = cache.get(key);
      if (entry != null) {
        hitCount.increment();
        return entry.automata;
      }
    }
    missCount.increment();

    // Build outside of the lock: this is the expensive part. Failures (invalid arguments, automata
    // too complex to determinize) propagate and are never cached.
    Automata built = build(term, maxEdits, prefixLength, transpositions);
    long entryRamBytes = ENTRY_OVERHEAD_RAM_BYTES + RamUsageEstimator.sizeOf(term);
    for (CompiledAutomaton automaton : built.automata) {
      entryRamBytes += automaton.ramBytesUsed();
    }
    if (maxSize == 0 || entryRamBytes > maxRamBytesUsed) {
      return built; // can never fit
    }

    synchronized (this) {
      Entry existing = cache.putIfAbsent(key, new Entry(built, entryRamBytes));
      if (existing != null) {
        return existing.automata; // another thread won the race
      }
      ramBytesUsed += entryRamBytes;
      // The new entry is the most recently used one, so it is evicted last and, since it fits on
      // its own, never by this loop.
      Iterator<Map.Entry<Key, Entry>> it = cache.entrySet().iterator();
      while (cache.size() > maxSize || ramBytesUsed > maxRamBytesUsed) {
        Entry evicted = it.next().getValue();
        it.remove();
        ramBytesUsed -= evicted.ramBytesUsed;
        evictionCount++;
      }
    }
    return built;
  }

  /** Removes all entries. Statistics are kept. */
  public synchronized void clear() {
    cache.clear();
    ramBytesUsed = 0;
  }

  /** Returns the number of entries currently in the cache. */
  public synchronized int size() {
    return cache.size();
  }

  /** Returns the number of lookups that were answered from the cache. */
  public long getHitCount() {
    return hitCount.sum();
  }

  /** Returns the number of lookups that had to build automata. */
  public long getMissCount() {
    return missCount.sum();
  }

  /** Returns the number of entries that were evicted to respect the bounds of the cache. */
  public synchronized long getEvictionCount() {
    return evictionCount;
  }

  @Override
  public synchronized long ramBytesUsed() {
    return BASE_RAM_BYTES + ramBytesUsed;
  }
}
