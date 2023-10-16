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
package org.apache.lucene.util.fst;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import org.apache.lucene.util.packed.PagedGrowableWriter;
import org.apache.lucene.util.packed.PackedInts;;

// nocommit we could gradually grow the double-barrel hash size based on size of the growing FST?  hmm does not work so well since you
// failed to use the larger hash in the beginning?  it's fundamentally needing two passes?

// TODO: any way to make a reversee suffix lookup (msokolov's idea) instead of more costly hash?  hmmm, though, hash is not so wasteful
// since it does not have to store value of each entry: the value is the node pointer in the FST.  actually, there is much to save
// there -- we would not need any long per entry -- we'd be able to start at the FST end node and work backwards from the transitions

// TODO: couldn't we prune natrually babck until we see a transition with an output?  it's highly unlikely (mostly impossible) such suffixes
// can be shared?

// nocommit: switch to growable packed thingy to store the double-barrel hash?

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  // primary table -- we add nodes into this until it reaches the requested tableSizeLimit/2, then we move it to fallback
  private PagedGrowableHash primaryTable;

  // how many nodes are allowed to store in both primary and fallback tables; when primary gets full (tableSizeLimit/2), we move it to the
  // fallback table
  private final long tableSizeLimit;

  // fallback table.  if we fallback and find the frozen node here, we promote it to primary table, for a simplistic and lowish-RAM-overhead
  // (compared to e.g. LinkedHashMap) LRU behaviour
  private PagedGrowableHash fallbackTable;

  private final FST<T> fst;
  private final FST.Arc<T> scratchArc = new FST.Arc<>();
  private final FST.BytesReader in;

  // nocommit just make huge tableSizeLimit to get minimality ... since it grows on demand, that's safe
  
  /** If tableSizeLimit is -1, we hold all node suffixes in the hash, and the FST is purely minimal.  If it's 0, we don't hash anything and the
   *  FST shares no suffixes.  If it's > 0, we create a simple LRU cache and the FST is "somewhat" minimal, moreso the larger the tableSizeLimit. */
  public NodeHash(FST<T> fst, long tableSizeLimit, FST.BytesReader in) {
    if (tableSizeLimit < 4) {
      // 2 is a power of 2, but does not work because the rehash logic (at 2/3 capacity) becomes over-quantized, and the hash
      // table becomes 100% full before moving to fallback, and then looking up an entry in 100% full hash table spins forever
      throw new IllegalArgumentException("tableSizeLimit must be at least 4; got: " + tableSizeLimit);
    }
    // nocommit should i insist specific limits (power of 2?) for most efficient RAM usage?  we want to push primary to fallback just before a rehash would
    // have occurred?
    primaryTable = new PagedGrowableHash();
    this.tableSizeLimit = tableSizeLimit;
    this.fst = fst;
    this.in = in;
  }

  // nocommit measure how wasteful/conflicty these hash tables are.  should we improve hash function?
  
  private long getFallback(FSTCompiler.UnCompiledNode<T> nodeIn, long hash) throws IOException {
    if (fallbackTable == null) {
      // no fallback yet (primary table is not yet large enough to swap)
      return 0;
    }
    long pos = hash & fallbackTable.mask;
    int c = 0;
    while (true) {
      long node = fallbackTable.get(pos);
      if (node == 0) {
        // not found
        return 0;
      } else if (nodesEqual(nodeIn, node)) {
        // frozen version of this node is already here
        return node;
      }

      // nocommit -- is this really quadratic probe?
      // quadratic probe
      pos = (pos + (++c)) & fallbackTable.mask;
    }
  }

  public long add(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn)
      throws IOException {

    long hash = hash(nodeIn);
    
    long pos = hash & primaryTable.mask;
    int c = 0;

    while (true) {

      long node = primaryTable.get(pos);
      if (node == 0) {
        // node is not in primary table; is it in fallback table?
        node = getFallback(nodeIn, hash);
        if (node != 0) {
          // it was already in fallback -- promote to primary
          primaryTable.set(pos, node);
        } else {
          // not in fallback either -- freeze & add the incoming node
        
          // freeze & add
          node = fst.addNode(fstCompiler, nodeIn);

          // we use 0 as empty marker in hash table, so it better be impossible to get a frozen node at 0:
          assert node != 0;

          // confirm frozen hash and unfrozen hash are the same
          assert hash(node) == hash : "mismatch frozenHash=" + hash(node) + " vs hash=" + hash;

          primaryTable.set(pos, node);
        }

        if (primaryTable.count > tableSizeLimit / 2) {
          // primary table is now too big -- swap to fallback table
          // nocommit -- when we do this, just allocate primary table to full hash size?  why pay cost of all the rehashing over and over?
          System.out.println("now fallback at count=" + primaryTable.count);
          fallbackTable = primaryTable;
          primaryTable = new PagedGrowableHash();
        }

        return node;
        
      } else if (nodesEqual(nodeIn, node)) {
        // same node (in frozen form) is already in primary table
        return node;
      }

      // nocommit -- is this really quadratic probe?
      // quadratic probe
      pos = (pos + (++c)) & primaryTable.mask;
    }
  }

  // hash code for an unfrozen node.  This must be identical
  // to the frozen case (below)!!
  private long hash(FSTCompiler.UnCompiledNode<T> node) {
    final int PRIME = 31;
    long h = 0;
    // TODO: maybe if number of arcs is high we can safely subsample?
    for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
      final FSTCompiler.Arc<T> arc = node.arcs[arcIdx];
      h = PRIME * h + arc.label;
      long n = ((FSTCompiler.CompiledNode) arc.target).node;
      h = PRIME * h + (int) (n ^ (n >> 32));
      h = PRIME * h + arc.output.hashCode();
      h = PRIME * h + arc.nextFinalOutput.hashCode();
      if (arc.isFinal) {
        h += 17;
      }
    }

    return h & Long.MAX_VALUE;
  }

  // hash code for a frozen node.  this must precisely match the hash computation of an unfrozen node!
  private long hash(long node) throws IOException {
    final int PRIME = 31;

    long h = 0;
    fst.readFirstRealTargetArc(node, scratchArc, in);
    while (true) {
      h = PRIME * h + scratchArc.label();
      h = PRIME * h + (int) (scratchArc.target() ^ (scratchArc.target() >> 32));
      h = PRIME * h + scratchArc.output().hashCode();
      h = PRIME * h + scratchArc.nextFinalOutput().hashCode();
      if (scratchArc.isFinal()) {
        h += 17;
      }
      if (scratchArc.isLast()) {
        break;
      }
      fst.readNextRealArc(scratchArc, in);
    }

    return h & Long.MAX_VALUE;
  }

  /** Compares an unfrozen node (UnCompiledNode) with a frozen node at byte location address (long), returning
   *  true if they are equal. */
  private boolean nodesEqual(FSTCompiler.UnCompiledNode<T> node, long address) throws IOException {
    fst.readFirstRealTargetArc(address, scratchArc, in);

    // fail fast for a node with fixed length arcs
    if (scratchArc.bytesPerArc() != 0) {
      assert node.numArcs > 0;
      // the frozen node uses fixed-with arc encoding (same number of bytes per arc), but may be sparse or dense
      switch (scratchArc.nodeFlags()) {
      case FST.ARCS_FOR_BINARY_SEARCH:
        // sparse
        if (node.numArcs != scratchArc.numArcs()) {
          return false;
        }
        break;
      case FST.ARCS_FOR_DIRECT_ADDRESSING:
        // dense -- compare both the number of labels allocated in the array (some of which may not actually be arcs), and the number of arcs
        if ((node.arcs[node.numArcs - 1].label - node.arcs[0].label + 1) != scratchArc.numArcs()
            || node.numArcs != FST.Arc.BitTable.countBits(scratchArc, in)) {
          return false;
        }
        break;
      default:
        throw new AssertionError("unhandled scratchArc.nodeFlag() " + scratchArc.nodeFlags());
      }
    }

    // compare arc by arc to see if there is a difference
    for (int arcUpto = 0; arcUpto < node.numArcs; arcUpto++) {
      final FSTCompiler.Arc<T> arc = node.arcs[arcUpto];
      if (arc.label != scratchArc.label()
          || arc.output.equals(scratchArc.output()) == false
          || ((FSTCompiler.CompiledNode) arc.target).node != scratchArc.target()
          || arc.nextFinalOutput.equals(scratchArc.nextFinalOutput()) == false
          || arc.isFinal != scratchArc.isFinal()) {
        return false;
      }

      if (scratchArc.isLast()) {
        if (arcUpto == node.numArcs - 1) {
          return true;
        } else {
          return false;
        }
      }

      fst.readNextRealArc(scratchArc, in);
    }

    // unfrozen node has fewer arcs than frozen node
    
    return false;
  }

  /** Inner class because it needs access to hash function and FST bytes. */
  private class PagedGrowableHash {
    private PagedGrowableWriter entries;
    private long count;
    private long mask;

    public PagedGrowableHash() {
      // nocommit should we reduce this 1 << 27 page size!?
      // allocate initially to store 16 elements, with page size 1 << 27 (128 MB!?), with 8 bits per value
      // nocommit this allocates a small block as the final block, but, what is the reallocation strategy as we append values beyond the end?
      entries = new PagedGrowableWriter(16, 1 << 27, 8, PackedInts.COMPACT);
      mask = 15;
    }

    public long get(long index) {
      return entries.get(index);
    }

    public void set(long index, long pointer) throws IOException {
      entries.set(index, pointer);
      count++;
      if (count > entries.size() * (2f / 3)) {
        // rehash at 2/3 occupancy
        rehash(pointer);
      }
    }

    private void rehash(long lastNodeAddress) throws IOException {
      // double hash table size on each rehash
      PagedGrowableWriter newEntries = new PagedGrowableWriter(
                                                               2 * entries.size(),
                                                               1 << 27,
                                                               PackedInts.bitsRequired(lastNodeAddress),
                                                               PackedInts.COMPACT);
      System.out.println("rehash primary to " + newEntries.size() + " entries");
                         
      long newMask = newEntries.size() - 1;
      for (long idx = 0; idx < entries.size(); idx++) {
        long address = entries.get(idx);
        if (address != 0) {
          long pos = hash(address) & newMask;
          int c = 0;
          while (true) {
            if (newEntries.get(pos) == 0) {
              newEntries.set(pos, address);
              break;
            }

            // quadratic probe
            pos = (pos + (++c)) & newMask;
          }
        }
      }

      mask = newMask;
      entries = newEntries;
      long ramBytesUsed = entries.ramBytesUsed();
      if (fallbackTable != null) {
        ramBytesUsed += fallbackTable.entries.ramBytesUsed();
      }
      System.out.println("RAM: " + ramBytesUsed + " bytes");
    }
  }
}

