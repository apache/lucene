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
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;

// TODO: any way to make a reverse suffix lookup (msokolov's idea) instead of more costly hash?
// hmmm, though, hash is not so wasteful
// since it does not have to store value of each entry: the value is the node pointer in the FST.
// actually, there is much to save
// there -- we would not need any long per entry -- we'd be able to start at the FST end node and
// work backwards from the transitions

// TODO: couldn't we prune naturally back until we see a transition with an output?  it's highly
// unlikely (mostly impossible) such suffixes can be shared?

// Used to dedup states (lookup already-frozen states)
final class NodeHash<T> {

  // primary table -- we add nodes into this until it reaches the requested tableSizeLimit/2, then
  // we move it to fallback
  private PagedGrowableHash primaryTable;

  // how many nodes are allowed to store in both primary and fallback tables; when primary gets full
  // (tableSizeLimit/2), we move it to the
  // fallback table
  private final long ramLimitBytes;

  // fallback table.  if we fallback and find the frozen node here, we promote it to primary table,
  // for a simplistic and lowish-RAM-overhead
  // (compared to e.g. LinkedHashMap) LRU behaviour.  fallbackTable is read-only.
  private PagedGrowableHash fallbackTable;

  private final FSTCompiler<T> fstCompiler;
  private final FST.Arc<T> scratchArc = new FST.Arc<>();
  // store the last fallback table node length in getFallback()
  private int lastFallbackNodeLength;
  // store the last fallback table hashtable position in getFallback()
  private long lastFallbackPos;

  /**
   * ramLimitMB is the max RAM we can use for recording suffixes. If we hit this limit, the least
   * recently used suffixes are discarded, and the FST is no longer minimalI. Still, larger
   * ramLimitMB will make the FST smaller (closer to minimal).
   */
  public NodeHash(FSTCompiler<T> fstCompiler, double ramLimitMB) {
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
    this.fstCompiler = fstCompiler;
  }

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
      } else {
        int length = fallbackTable.getMatchedNodeLength(nodeIn, node, pos);
        if (length != -1) {
          // store the node length for further use
          this.lastFallbackNodeLength = length;
          this.lastFallbackPos = pos;
          // frozen version of this node is already here
          return node;
        }
      }

      // quadratic probe (but is it, really?)
      pos = (pos + (++c)) & fallbackTable.mask;
    }
  }

  public long add(FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {

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
          // TODO: Copy directly between 2 ByteBlockPool to avoid double-copy
          primaryTable.set(
              pos, node, fallbackTable.getBytes(lastFallbackPos, lastFallbackNodeLength));
        } else {
          // not in fallback either -- freeze & add the incoming node

          long startAddress = fstCompiler.bytes.getPosition();
          // freeze & add
          node = fstCompiler.addNode(nodeIn);

          // TODO: Write the bytes directly from BytesStore
          // we use 0 as empty marker in hash table, so it better be impossible to get a frozen node
          // at 0:
          assert node != FST.FINAL_END_NODE && node != FST.NON_FINAL_END_NODE;
          byte[] buf = new byte[Math.toIntExact(node - startAddress + 1)];
          fstCompiler.bytes.copyBytes(startAddress, buf, 0, buf.length);

          primaryTable.set(pos, node, buf);

          // confirm frozen hash and unfrozen hash are the same
          assert primaryTable.hash(node, pos) == hash
              : "mismatch frozenHash=" + primaryTable.hash(node, pos) + " vs hash=" + hash;
        }

        // how many bytes would be used if we had "perfect" hashing:
        //  - x2 for fstNodeAddress for FST node address
        //  - x2 for copiedNodeAddress for copied node address
        // each account for approximate hash table overhead halfway between 33.3% and 66.6%
        // note that some of the copiedNodes are shared between fallback and primary tables so this
        // computation is pessimistic
        long ramBytesUsed =
            primaryTable.count * 2 * PackedInts.bitsRequired(node) / 8
                + primaryTable.count * 2 * PackedInts.bitsRequired(primaryTable.copiedBytes) / 8
                + primaryTable.copiedBytes;

        // NOTE: we could instead use the more precise RAM used, but this leads to unpredictable
        // quantized behavior due to 2X rehashing where for large ranges of the RAM limit, the
        // size of the FST does not change, and then suddenly when you cross a secret threshold,
        // it drops.  With this approach (measuring "perfect" hash storage and approximating the
        // overhead), the behaviour is more strictly monotonic: larger RAM limits smoothly result
        // in smaller FSTs, even if the precise RAM used is not always under the limit.

        // divide limit by 2 because fallback gets half the RAM and primary gets the other half
        if (ramBytesUsed >= ramLimitBytes / 2) {
          // time to fallback -- fallback is now used read-only to promote a node (suffix) to
          // primary if we encounter it again
          fallbackTable = primaryTable;
          // size primary table the same size to reduce rehash cost
          // TODO: we could clear & reuse the previous fallbackTable, instead of allocating a new
          //       to reduce GC load
          primaryTable =
              new PagedGrowableHash(node, Math.max(16, primaryTable.fstNodeAddress.size()));
        } else if (primaryTable.count > primaryTable.fstNodeAddress.size() * (2f / 3)) {
          // rehash at 2/3 occupancy
          primaryTable.rehash(node);
        }

        return node;

      } else if (primaryTable.getMatchedNodeLength(nodeIn, node, pos) != -1) {
        // same node (in frozen form) is already in primary table
        return node;
      }

      // quadratic probe (but is it, really?)
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

    return h;
  }

  /** Inner class because it needs access to hash function and FST bytes. */
  private class PagedGrowableHash {
    public long copiedBytes;
    // storing the FST node address where the position is the masked hash of the node arcs
    private PagedGrowableWriter fstNodeAddress;
    // storing the local copiedNodes address in the same position as fstNodeAddress
    // here we are effectively storing a Map<Long, Long> from the FST node address to copiedNodes
    // address
    private PagedGrowableWriter copiedNodeAddress;
    private long count;
    private long mask;
    // storing the byte slice from the FST for nodes we added to the hash so that we don't need to
    // look up from the FST itself. each node will be written subsequently
    private final ByteBlockPool copiedNodes;

    // 256K blocks, but note that the final block is sized only as needed so it won't use the full
    // block size when just a few elements were written to it
    private static final int BLOCK_SIZE_BYTES = 1 << 18;

    public PagedGrowableHash() {
      fstNodeAddress = new PagedGrowableWriter(16, BLOCK_SIZE_BYTES, 8, PackedInts.COMPACT);
      copiedNodeAddress = new PagedGrowableWriter(16, BLOCK_SIZE_BYTES, 8, PackedInts.COMPACT);
      mask = 15;
      copiedNodes = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    }

    public PagedGrowableHash(long lastNodeAddress, long size) {
      fstNodeAddress =
          new PagedGrowableWriter(
              size, BLOCK_SIZE_BYTES, PackedInts.bitsRequired(lastNodeAddress), PackedInts.COMPACT);
      copiedNodeAddress = new PagedGrowableWriter(size, BLOCK_SIZE_BYTES, 8, PackedInts.COMPACT);
      mask = size - 1;
      assert (mask & size) == 0 : "size must be a power-of-2; got size=" + size + " mask=" + mask;
      copiedNodes = new ByteBlockPool(new ByteBlockPool.DirectAllocator());
    }

    public byte[] getBytes(long pos, int length) {
      long address = copiedNodeAddress.get(pos);
      assert address - length + 1 >= 0;
      byte[] buf = new byte[length];
      copiedNodes.readBytes(address - length + 1, buf, 0, length);
      return buf;
    }

    public long get(long index) {
      return fstNodeAddress.get(index);
    }

    public void set(long index, long pointer, byte[] bytes) {
      fstNodeAddress.set(index, pointer);
      count++;
      copiedNodes.append(new BytesRef(bytes));
      copiedBytes += bytes.length;
      // write the offset, which is the last offset of the node
      copiedNodeAddress.set(index, copiedBytes - 1);
    }

    private void rehash(long lastNodeAddress) throws IOException {
      // TODO: https://github.com/apache/lucene/issues/12744
      // should we always use a small startBitsPerValue here (e.g 8) instead base off of
      // lastNodeAddress?

      // double hash table size on each rehash
      long newSize = 2 * fstNodeAddress.size();
      PagedGrowableWriter newCopiedOffsets =
          new PagedGrowableWriter(
              newSize, BLOCK_SIZE_BYTES, PackedInts.bitsRequired(copiedBytes), PackedInts.COMPACT);
      PagedGrowableWriter newEntries =
          new PagedGrowableWriter(
              newSize,
              BLOCK_SIZE_BYTES,
              PackedInts.bitsRequired(lastNodeAddress),
              PackedInts.COMPACT);
      long newMask = newEntries.size() - 1;
      for (long idx = 0; idx < fstNodeAddress.size(); idx++) {
        long address = fstNodeAddress.get(idx);
        if (address != 0) {
          long pos = hash(address, idx) & newMask;
          int c = 0;
          while (true) {
            if (newEntries.get(pos) == 0) {
              newEntries.set(pos, address);
              newCopiedOffsets.set(pos, copiedNodeAddress.get(idx));
              break;
            }

            // quadratic probe
            pos = (pos + (++c)) & newMask;
          }
        }
      }

      mask = newMask;
      fstNodeAddress = newEntries;
      copiedNodeAddress = newCopiedOffsets;
    }

    // hash code for a frozen node.  this must precisely match the hash computation of an unfrozen
    // node!
    private long hash(long node, long pos) throws IOException {
      FST.BytesReader in = getBytesReader(node, pos);

      final int PRIME = 31;

      long h = 0;
      fstCompiler.fst.readFirstRealTargetArc(node, scratchArc, in);
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
        fstCompiler.fst.readNextRealArc(scratchArc, in);
      }

      return h;
    }

    /**
     * Compares an unfrozen node (UnCompiledNode) with a frozen node at byte location address
     * (long), returning the node length if the two nodes are matched, or -1 otherwise
     */
    private int getMatchedNodeLength(FSTCompiler.UnCompiledNode<T> node, long address, long pos)
        throws IOException {
      FST.BytesReader in = getBytesReader(address, pos);
      fstCompiler.fst.readFirstRealTargetArc(address, scratchArc, in);

      // fail fast for a node with fixed length arcs
      if (scratchArc.bytesPerArc() != 0) {
        assert node.numArcs > 0;
        // the frozen node uses fixed-with arc encoding (same number of bytes per arc), but may be
        // sparse or dense
        switch (scratchArc.nodeFlags()) {
          case FST.ARCS_FOR_BINARY_SEARCH:
            // sparse
            if (node.numArcs != scratchArc.numArcs()) {
              return -1;
            }
            break;
          case FST.ARCS_FOR_DIRECT_ADDRESSING:
            // dense -- compare both the number of labels allocated in the array (some of which may
            // not actually be arcs), and the number of arcs
            if ((node.arcs[node.numArcs - 1].label - node.arcs[0].label + 1) != scratchArc.numArcs()
                || node.numArcs != FST.Arc.BitTable.countBits(scratchArc, in)) {
              return -1;
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
          return -1;
        }

        if (scratchArc.isLast()) {
          if (arcUpto == node.numArcs - 1) {
            // position is 1 index past the starting address, as we are reading in backward
            return Math.toIntExact(address - in.getPosition());
          } else {
            return -1;
          }
        }

        fstCompiler.fst.readNextRealArc(scratchArc, in);
      }

      // unfrozen node has fewer arcs than frozen node

      return -1;
    }

    private FST.BytesReader getBytesReader(long address, long pos) {
      long localAddress = copiedNodeAddress.get(pos);
      return new ByteBlockPoolReverseBytesReader(copiedNodes, address - localAddress);
    }
  }
}
