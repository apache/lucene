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

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  // nocommit
  private static final boolean DO_PRINT_HASH_RAM = true;

  // primary table -- we add nodes into this until it reaches the requested tableSizeLimit/2, then we move it to fallback
  private PagedGrowableHash primaryTable;

  // how many nodes are allowed to store in both primary and fallback tables; when primary gets full (tableSizeLimit/2), we move it to the
  // fallback table
  private final long ramLimitBytes;

  // fallback table.  if we fallback and find the frozen node here, we promote it to primary table, for a simplistic and lowish-RAM-overhead
  // (compared to e.g. LinkedHashMap) LRU behaviour
  private PagedGrowableHash fallbackTable;

  private final FST<T> fst;
  private final FST.Arc<T> scratchArc = new FST.Arc<>();
  private final FST.BytesReader in;

  /** ramLimitMB is the max RAM we can use for recording suffixes. If we hit this limit, the least recently used suffixes are discarded, and
   *  the FST is no longer minimalI.  Still, larger ramLimitMB will make the FST smaller (closer to minimal). */
  public NodeHash(FST<T> fst, double ramLimitMB, FST.BytesReader in) {
    if (ramLimitMB <= 0) {
      throw new IllegalArgumentException("ramLimitMB must be > 0; got: " + ramLimitMB);
    }
    double asBytes = ramLimitMB * 1024 * 1024;
    if (asBytes >= Long.MAX_VALUE) {
      // quietly truncate to Long.MAX_VALUE in bytes too
      ramLimitBytes = Long.MAX_VALUE;
    } else {
      ramLimitBytes = (long) asBytes;
    }
    
    primaryTable = new PagedGrowableHash();
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

        if (primaryTable.count > primaryTable.entries.size() * (3f / 4)) {
          // rehash at 3/4 occupancy

          // NOTE that this is still not perfect, e.g. if we are able to rehash, but then bpv increases for newly inserted nodes into the
          // primary hash), then the primary hash can use more than 2X its current RAM
          
          long ramUsedAfterRehash = 2 * primaryTable.entries.ramBytesUsed();
          if (fallbackTable != null) {
            ramUsedAfterRehash += fallbackTable.entries.ramBytesUsed();
          } else {
            // when primary becomes fallback after this pending rehash, both will use this much RAM: (because on fallback we size the
            // new primary to 1/2 of the current primary)
            ramUsedAfterRehash += 0.5 * primaryTable.entries.ramBytesUsed();;
          }
          System.out.println("if we were to rehash from ram=" + primaryTable.entries.ramBytesUsed() + ": " + String.format(Locale.ROOT, "%.1f", (ramUsedAfterRehash / 1024. / 1024.)));
          if (ramUsedAfterRehash >= ramLimitBytes) {
            // rehashing will just immediately force fallback, so we just fallback now instead
            fallbackTable = primaryTable;
            // make the new primary table half sized to stay under the alloted RAM budget since fallbackTable will grow in RAM mb, even at
            // fixed size, since bpv is increasing as node addresses get bigger
            primaryTable = new PagedGrowableHash(node, Math.max(16, primaryTable.entries.size()/2));

            long estNumBytesPrimary = primaryTable.entries.size() * PackedInts.bitsRequired(node) / 8;
            
            System.out.println("fallback: RAM " + (fallbackTable.entries.ramBytesUsed() + estNumBytesPrimary));
          } else {
            // we still have room to grow, so we rehash the primary table and do not drop any LRU entries, yet
            primaryTable.rehash(node);
            if (DO_PRINT_HASH_RAM) {
              long ramUsed = primaryTable.entries.ramBytesUsed();
              if (fallbackTable != null) {
                ramUsed += fallbackTable.entries.ramBytesUsed();
              }
              System.out.println("rehash: RAM " + ramUsed + " bytes");
            }
          }
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

    // nocommi this used to be 1 << 27, which seems too big -- all values in a block must use the same bpv?
    // 256K blocks, but note that the final block is sized only as needed so it won't use the full block size when just a few elements were
    // written to it
    private static final int BLOCK_SIZE_BYTES = 1 << 18;

    public PagedGrowableHash() {
      entries = new PagedGrowableWriter(16, BLOCK_SIZE_BYTES, 8, PackedInts.COMPACT);
      mask = 15;
    }

    public PagedGrowableHash(long lastNodeAddress, long size) {
      entries = new PagedGrowableWriter(size, BLOCK_SIZE_BYTES, PackedInts.bitsRequired(lastNodeAddress), PackedInts.COMPACT);
      mask = size - 1;
      assert (mask % size) == 0;
    }

    public long get(long index) {
      return entries.get(index);
    }

    public void set(long index, long pointer) throws IOException {
      entries.set(index, pointer);
      count++;
    }

    private void rehash(long lastNodeAddress) throws IOException {
      // double hash table size on each rehash
      PagedGrowableWriter newEntries = new PagedGrowableWriter(
                                                               2 * entries.size(),
                                                               BLOCK_SIZE_BYTES,
                                                               PackedInts.bitsRequired(lastNodeAddress),
                                                               PackedInts.COMPACT);
      if (DO_PRINT_HASH_RAM) {
        System.out.println("rehash primary to " + newEntries.size() + " entries");
      }
                         
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
    }
  }
}

