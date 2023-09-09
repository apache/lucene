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

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  // primary table
  private long[] table;

  // how many nodes are stored in the primary table; when this gets full, we discard tableOld and move primary to it
  private long count;

  // fallback table.  if we fallback and find the frozen node here, we promote it to primary table, for a simplistic LRU behaviour
  private long[] fallbackTable;

  private final int mask;
  private final FST<T> fst;
  private final FST.Arc<T> scratchArc = new FST.Arc<>();
  private final FST.BytesReader in;

  public NodeHash(FST<T> fst, int tableSize, FST.BytesReader in) {
    if (tableSize <= 0) {
      throw new IllegalArgumentException("tableSize must be positive; got: " + tableSize);
    }
    mask = tableSize - 1;
    if ((mask & tableSize) != 0) {
      throw new IllegalArgumentException("tableSize must be a power of 2; got: " + tableSize);
    }
    table = new long[tableSize];
    fallbackTable = new long[tableSize];
    this.fst = fst;
    this.in = in;
  }

  /** Compares an unfrozen node (UnCompiledNode) with a frozen node at byte location address, returning
   *  true if they are equal. */
  private boolean nodesEqual(FSTCompiler.UnCompiledNode<T> node, long address) throws IOException {
    fst.readFirstRealTargetArc(address, scratchArc, in);

    // fail fast for a node with fixed length arcs
    if (scratchArc.bytesPerArc() != 0) {
      // the frozen node uses fixed-with arc encoding (same number of bytes per arc), but may be sparse or dense
      switch (scratchArc.nodeFlags()) {
        case FST.ARCS_FOR_BINARY_SEARCH:
          if (node.numArcs != scratchArc.numArcs()) {
            return false;
          }
          break;
        case FST.ARCS_FOR_DIRECT_ADDRESSING:
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

  // nocommit lets store the (cached) hash value for nodes in the new fixed-size hash table?
  
  // hash code for a frozen node
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

  // nocommit measure how wasteful/conflicty these hash tables are.  should we improve hash function?
  // nocommit should i make hash a long?
  
  private long getFallback(FSTCompiler.UnCompiledNode<T> nodeIn, long hash) throws IOException {
    int pos = (int) (hash & mask);
    int c = 0;
    while (true) {
      long node = fallbackTable[pos];
      if (node == 0) {
        // not found
        return 0;
      } else if (nodesEqual(nodeIn, node)) {
        // frozen version of this node is already here
        return node;
      }

      // nocommit -- is this really quadratic probe?
      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }

  public long add(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn)
      throws IOException {

    long hash = hash(nodeIn);
    
    int pos = (int) (hash & mask);
    int c = 0;

    while (true) {

      long node = table[pos];
      if (node == 0) {
        // node is not in primary table; is it in fallback table?
        node = getFallback(nodeIn, hash);
        if (node != 0) {
          // it was already in fallback -- promote
          // nocommit could we somehow use the pos from fallback here?  i don't think so?  this hash will have different entries
          //System.out.println("promote fallback " + node);
          table[pos] = node;
        } else {
          // not in fallback either -- freeze & add the incoming node
        
          // freeze & add
          node = fstCompiler.addNode(nodeIn);

          // we use 0 as empty marker in hash table, so it better be impossible to get a frozen node at 0:
          assert node != 0;

          // confirm frozen hash and unfrozen hash are the same
          assert hash(node) == hash : "frozenHash=" + hash(node) + " vs hash=" + hash;

          table[pos] = node;
        }

        count++;

        // swap with fallback at 2/3 occupancy
        if (count > 2 * table.length / 3) {
          // more current table to fallback, swap old fallback to current table and zero/clear it
          long[] tmp = fallbackTable;
          fallbackTable = table;
          table = tmp;
          Arrays.fill(table, 0);
          count = 0;
        }

        return node;
        
      } else if (nodesEqual(nodeIn, node)) {
        // same node (in frozen form) is already in primary table
        //System.out.println("found existing " + node);
        return node;
      }

      // nocommit -- is this really quadratic probe?
      // quadratic probe
      pos = (pos + (++c)) & mask;
    }
  }
}
