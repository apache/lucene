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
package org.apache.lucene.search.suggest.fst;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;

/**
 * Finite state automata based implementation of "autocomplete" functionality.
 *
 * @see FSTCompletionBuilder
 * @lucene.experimental
 */

// TODO: we could store exact weights as outputs from the FST (int4 encoded
// floats). This would provide exact outputs from this method and to some
// degree allowed post-sorting on a more fine-grained weight.

// TODO: support for Analyzers (infix suggestions, synonyms?)

public class FSTCompletion {
  /** A single completion for a given key. */
  public static final class Completion implements Comparable<Completion> {
    /** UTF-8 bytes of the suggestion */
    public final BytesRef utf8;
    /** source bucket (weight) of the suggestion */
    public final int bucket;

    Completion(BytesRef key, int bucket) {
      this.utf8 = BytesRef.deepCopyOf(key);
      this.bucket = bucket;
    }

    @Override
    public String toString() {
      return utf8.utf8ToString() + "/" + bucket;
    }

    /** Completions are equal when their {@link #utf8} images are equal (bucket is not compared). */
    @Override
    public int compareTo(Completion o) {
      return this.utf8.compareTo(o.utf8);
    }
  }

  /** Default number of buckets. */
  public static final int DEFAULT_BUCKETS = 10;

  /**
   * An empty result. Keep this an {@link ArrayList} to keep all the returned lists of single type
   * (monomorphic calls).
   */
  private static final ArrayList<Completion> EMPTY_RESULT = new ArrayList<>();

  /** Finite state automaton encoding all the lookup terms. See class notes for details. */
  private final FST<Object> automaton;

  /**
   * An array of arcs leaving the root automaton state and encoding weights of all completions in
   * their sub-trees.
   */
  private final Arc<Object>[] rootArcs;

  /** @see #FSTCompletion(FST, boolean, boolean) */
  private boolean exactFirst;

  /** @see #FSTCompletion(FST, boolean, boolean) */
  private boolean higherWeightsFirst;

  /**
   * Constructs an FSTCompletion, specifying higherWeightsFirst and exactFirst.
   *
   * @param automaton Automaton with completions. See {@link FSTCompletionBuilder}.
   * @param higherWeightsFirst Return most popular suggestions first. This is the default behavior
   *     for this implementation. Setting it to <code>false</code> has no effect (use constant term
   *     weights to sort alphabetically only).
   * @param exactFirst Find and push an exact match to the first position of the result list if
   *     found.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public FSTCompletion(FST<Object> automaton, boolean higherWeightsFirst, boolean exactFirst) {
    this.automaton = automaton;
    if (automaton != null) {
      this.rootArcs = cacheRootArcs(automaton);
    } else {
      this.rootArcs = new Arc[0];
    }
    this.higherWeightsFirst = higherWeightsFirst;
    this.exactFirst = exactFirst;
  }

  /**
   * Defaults to higher weights first and exact first.
   *
   * @see #FSTCompletion(FST, boolean, boolean)
   */
  public FSTCompletion(FST<Object> automaton) {
    this(automaton, true, true);
  }

  /** Cache the root node's output arcs starting with completions with the highest weights. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Arc<Object>[] cacheRootArcs(FST<Object> automaton) {
    try {
      List<Arc<Object>> rootArcs = new ArrayList<>();
      Arc<Object> arc = automaton.getFirstArc(new Arc<>());
      FST.BytesReader fstReader = automaton.getBytesReader();
      automaton.readFirstTargetArc(arc, arc, fstReader);
      while (true) {
        rootArcs.add(new Arc<>().copyFrom(arc));
        if (arc.isLast()) break;
        automaton.readNextArc(arc, fstReader);
      }

      Collections.reverse(rootArcs); // we want highest weights first.
      return rootArcs.toArray(new Arc[rootArcs.size()]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the first exact match by traversing root arcs, starting from the arc <code>rootArcIndex
   * </code>.
   *
   * @param rootArcIndex The first root arc index in {@link #rootArcs} to consider when matching.
   * @param utf8 The sequence of utf8 bytes to follow.
   * @return Returns the bucket number of the match or <code>-1</code> if no match was found.
   */
  private int getExactMatchStartingFromRootArc(int rootArcIndex, BytesRef utf8) {
    // Get the UTF-8 bytes representation of the input key.
    try {
      final FST.Arc<Object> scratch = new FST.Arc<>();
      FST.BytesReader fstReader = automaton.getBytesReader();
      for (; rootArcIndex < rootArcs.length; rootArcIndex++) {
        final FST.Arc<Object> rootArc = rootArcs[rootArcIndex];
        final FST.Arc<Object> arc = scratch.copyFrom(rootArc);

        // Descend into the automaton using the key as prefix.
        if (descendWithPrefix(arc, utf8)) {
          automaton.readFirstTargetArc(arc, arc, fstReader);
          if (arc.label() == FST.END_LABEL) {
            // Normalize prefix-encoded weight.
            return rootArc.label();
          }
        }
      }
    } catch (IOException e) {
      // Should never happen, but anyway.
      throw new RuntimeException(e);
    }

    // No match.
    return -1;
  }

  /**
   * Lookup suggestions to <code>key</code>.
   *
   * @param key The prefix to which suggestions should be sought.
   * @param num At most this number of suggestions will be returned.
   * @return Returns the suggestions, sorted by their approximated weight first (decreasing) and
   *     then alphabetically (UTF-8 codepoint order).
   */
  public List<Completion> lookup(CharSequence key, int num) {
    if (key.length() == 0 || automaton == null) {
      return EMPTY_RESULT;
    }

    if (!higherWeightsFirst && rootArcs.length > 1) {
      // We could emit a warning here (?). An optimal strategy for
      // alphabetically sorted
      // suggestions would be to add them with a constant weight -- this saves
      // unnecessary
      // traversals and sorting.
      return lookup(key).sorted().limit(num).collect(Collectors.toList());
    } else {
      return lookup(key).limit(num).collect(Collectors.toList());
    }
  }

  /**
   * Lookup suggestions to <code>key</code> and return a stream of matching completions. The stream
   * fetches completions dynamically - it can be filtered and limited to acquire the desired number
   * of completions without collecting all of them.
   *
   * @param key The prefix to which suggestions should be sought.
   * @return Returns the suggestions
   */
  public Stream<Completion> lookup(CharSequence key) {
    if (key.length() == 0 || automaton == null) {
      return Stream.empty();
    }

    try {
      return lookupSortedByWeight(new BytesRef(key));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Lookup suggestions sorted by weight (descending order). */
  private Stream<Completion> lookupSortedByWeight(BytesRef key) throws IOException {

    // Look for an exact match first.
    Completion exactCompletion;
    if (exactFirst) {
      Completion c = null;
      for (int i = 0; i < rootArcs.length; i++) {
        int exactMatchBucket = getExactMatchStartingFromRootArc(i, key);
        if (exactMatchBucket != -1) {
          // root arcs are sorted by decreasing weight so any first exact match will always win.
          c = new Completion(key, exactMatchBucket);
          break;
        }
      }
      exactCompletion = c;
    } else {
      exactCompletion = null;
    }

    Stream<Completion> stream =
        IntStream.range(0, rootArcs.length)
            .boxed()
            .flatMap(
                i -> {
                  try {
                    final FST.Arc<Object> rootArc = rootArcs[i];
                    final FST.Arc<Object> arc = new FST.Arc<>().copyFrom(rootArc);
                    if (descendWithPrefix(arc, key)) {
                      // A subgraph starting from the current node has the completions
                      // of the key prefix. The arc we're at is the last key's byte,
                      // so we will collect it too.
                      final BytesRef output = BytesRef.deepCopyOf(key);
                      output.length = key.length;
                      return completionStream(output, rootArc.label(), arc);
                    } else {
                      return Stream.empty();
                    }
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                });

    // if requested, return the exact completion first and omit it in any further completions.
    if (exactFirst && exactCompletion != null) {
      stream =
          Stream.concat(
              Stream.of(exactCompletion),
              stream.filter(completion -> exactCompletion.compareTo(completion) != 0));
    }
    return stream;
  }

  /** Return a stream of all completions starting from the provided arc. */
  private Stream<? extends Completion> completionStream(
      BytesRef output, int bucket, Arc<Object> fromArc) throws IOException {

    FST.BytesReader fstReader = automaton.getBytesReader();

    class State {
      Arc<Object> arc;
      int outputLength;

      State(Arc<Object> arc, int outputLength) throws IOException {
        this.arc = automaton.readFirstTargetArc(arc, new Arc<>(), fstReader);
        this.outputLength = outputLength;
      }
    }

    ArrayDeque<State> states = new ArrayDeque<>();
    states.addLast(new State(fromArc, output.length));

    return StreamSupport.stream(
        new Spliterator<>() {
          @Override
          public boolean tryAdvance(Consumer<? super Completion> action) {
            try {
              while (!states.isEmpty()) {
                var state = states.peekLast();
                output.length = state.outputLength;
                var arc = state.arc;
                var arcLabel = arc.label();

                if (arcLabel == FST.END_LABEL) {
                  Completion completion = new Completion(output, bucket);
                  action.accept(completion);

                  if (arc.isLast()) {
                    states.removeLast();
                  } else {
                    automaton.readNextArc(arc, fstReader);
                  }

                  return true;
                } else {
                  assert output.offset == 0;
                  if (output.length == output.bytes.length) {
                    output.bytes = ArrayUtil.grow(output.bytes);
                  }
                  output.bytes[output.length++] = (byte) arcLabel;

                  State newState = new State(arc, output.length);

                  if (arc.isLast()) {
                    states.removeLast();
                  } else {
                    automaton.readNextArc(arc, fstReader);
                  }

                  states.addLast(newState);
                }
              }

              return false;
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          @Override
          public Spliterator<Completion> trySplit() {
            // Don't try to split.
            return null;
          }

          @Override
          public long estimateSize() {
            return Long.MAX_VALUE;
          }

          @Override
          public int characteristics() {
            return Spliterator.NONNULL | Spliterator.ORDERED;
          }
        },
        false);
  }

  /**
   * Descend along the path starting at <code>arc</code> and going through bytes in the argument.
   *
   * @param arc The starting arc. This argument is modified in-place.
   * @param utf8 The term to descend along.
   * @return If <code>true</code>, <code>arc</code> will be set to the arc matching last byte of
   *     <code>term</code>. <code>false</code> is returned if no such prefix exists.
   */
  private boolean descendWithPrefix(Arc<Object> arc, BytesRef utf8) throws IOException {
    final int max = utf8.offset + utf8.length;
    // Cannot save as instance var since multiple threads
    // can use FSTCompletion at once...
    final FST.BytesReader fstReader = automaton.getBytesReader();
    for (int i = utf8.offset; i < max; i++) {
      if (automaton.findTargetArc(utf8.bytes[i] & 0xff, arc, arc, fstReader) == null) {
        // No matching prefixes, return an empty result.
        return false;
      }
    }
    return true;
  }

  /** Returns the bucket count (discretization thresholds). */
  public int getBucketCount() {
    return rootArcs.length;
  }

  /**
   * Returns the bucket assigned to a given key (if found) or <code>-1</code> if no exact match
   * exists.
   */
  public int getBucket(CharSequence key) {
    return getExactMatchStartingFromRootArc(0, new BytesRef(key));
  }

  /** Returns the internal automaton. */
  public FST<Object> getFST() {
    return automaton;
  }
}
