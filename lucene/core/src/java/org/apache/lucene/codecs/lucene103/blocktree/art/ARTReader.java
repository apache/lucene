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
package org.apache.lucene.codecs.lucene103.blocktree.art;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

/**
 * Visit or find(search) terms. We can read an ART from disk, or from root node directly(similar to
 * the usage of fst in org.apache.lucene.analysis.charfilter.NormalizeCharMap). This version save
 * node, output, floor data's fp, and load node by these fp.
 */
public class ARTReader {

  final RandomAccessInput access;
  final IndexInput input;
  public Node root;

  // For testing.
  public Node getRoot() {
    return root;
  }

  /** Just read root node. */
  public ARTReader(IndexInput input, long rootFP) throws IOException {
    this.access = input.randomAccessSlice(0, input.length());
    this.input = input;
    this.root = load(rootFP);
  }

  /** Read one node from access with specify fp. */
  private Node load(long fp) throws IOException {
    Node node = Node.load(access, fp);
    node.fp = fp;
    return node;
  }

  /**
   * Different with lookupChild, this method just find a child, ignore remaining bytes. So we should
   * compare parent's remaining bytes(prefix, key), and find child next.
   */
  public Node lookupChildLazily(BytesRef target, Node parent) throws IOException {
    assert parent != null;

    // TODO: Target length is 0 may never happen, when we search step by step?
    if (target.length == 0) {
      // We may search "";
      if (parent.nodeType == NodeType.LEAF_NODE && parent.key == null) {
        // Match, Use this parent's output.
        return null;
      } else if (parent.prefixLength == 0) {
        // Match, Use this parent's output.
        return null;
      } else {
        // Not match, keep in this parent.
        return parent;
      }
    }

    if (parent.nodeType.equals(NodeType.LEAF_NODE)) {
      if (parent.key.equals(target)) {
        // Match, Use this parent's output.
        return null;
      } else {
        // Not match, keep in this parent.
        return parent;
      }
    } else {
      if (parent.prefixLength > 0) {
        int commonLength =
            ARTUtil.commonPrefixLength(
                target.bytes,
                target.offset,
                target.offset + target.length,
                parent.prefix,
                0,
                parent.prefixLength);
        if (commonLength != parent.prefixLength) {
          // Not match, keep in this parent.
          return parent;
        }
        // common prefix is the same, then increase the offset.
        target.offset += parent.prefixLength;
        target.length -= parent.prefixLength;
        // Work end, match.
        if (target.length == 0) {
          // Match, Use this parent's output.
          return null;
        }
      }

      // Get child.
      byte indexByte = target.bytes[target.offset];
      int childPos = parent.getChildPos(target.bytes[target.offset]);
      target.offset++;
      target.length--;
      if (childPos != Node.ILLEGAL_IDX) {
        // For Node 256, there is gap in children, we need minus the number of null child from 0 to
        // this pos.
        // For Node 48, childPos is the child index byte, we need use the read index in children.
        long childDeltaFpStart;
        if (parent.nodeType.equals(NodeType.NODE48)) {
          int childIndex = ((Node48) parent).getChildIndex(indexByte);
          childDeltaFpStart =
              parent.childrenDeltaFpStart + (long) childIndex * parent.childrenDeltaFpBytes;
        } else if (parent.nodeType.equals(NodeType.NODE256)) {
          int numberOfNullChildren = ((Node256) parent).numberOfNullChildren(childPos);
          childDeltaFpStart =
              parent.childrenDeltaFpStart
                  + (long) (childPos - numberOfNullChildren) * parent.childrenDeltaFpBytes;
        } else {
          childDeltaFpStart =
              parent.childrenDeltaFpStart + (long) childPos * parent.childrenDeltaFpBytes;
        }
        long childDeltaFp = access.readLong(childDeltaFpStart);
        if (parent.childrenDeltaFpBytes < 8) {
          childDeltaFp = childDeltaFp & Node.BYTES_MINUS_1_MASK[parent.childrenDeltaFpBytes - 1];
        }

        long childFp = parent.fp - childDeltaFp;
        assert childFp >= 0 && childFp < parent.fp : "child fp should less than parent fp";
        return Node.load(access, childFp);
      } else {
        // Not match, keep in this parent.
        //        return parent;
        // If prefix is 0, and this indexByte has no child, and if this node has output, we should
        // scan
        // output's block.
        // If prefix is not 0, but target contains prefix (prefixLength equals commonLength), and if
        // this node has output, we still need to scan output's block.
        return null;
      }
    }
  }

  /**
   * Find the next child to search, note the child's prefix(non-leaf node) or key(leaf node) must
   * same with target. Returns: 1 null: can not find a child. 2 child: next node to search. 3
   * parent: we get a child, but remaining bytes are different.
   */
  public Node lookupChild(BytesRef target, Node parent) throws IOException {
    assert parent != null;

    // TODO: Target length is 0 may never happen, when we search step by step?
    if (target.length == 0) {
      // We can not get a child from empty target.
      return null;
    }

    if (parent.nodeType.equals(NodeType.LEAF_NODE)) {
      return null;
    } else {
      // Get child.
      byte indexByte = target.bytes[target.offset];
      int childPos = parent.getChildPos(target.bytes[target.offset]);
      target.offset++;
      target.length--;
      if (childPos != Node.ILLEGAL_IDX) {
        // For Node 256, there is gap in children, we need minus the number of null child from 0 to
        // this pos.
        // For Node 48, childPos is the child index byte, we need use the read index in children.
        long childDeltaFpStart;
        if (parent.nodeType.equals(NodeType.NODE48)) {
          int childIndex = ((Node48) parent).getChildIndex(indexByte);
          childDeltaFpStart =
              parent.childrenDeltaFpStart + (long) childIndex * parent.childrenDeltaFpBytes;
        } else if (parent.nodeType.equals(NodeType.NODE256)) {
          int numberOfNullChildren = ((Node256) parent).numberOfNullChildren(childPos);
          childDeltaFpStart =
              parent.childrenDeltaFpStart
                  + (long) (childPos - numberOfNullChildren) * parent.childrenDeltaFpBytes;
        } else {
          childDeltaFpStart =
              parent.childrenDeltaFpStart + (long) childPos * parent.childrenDeltaFpBytes;
        }
        long childDeltaFp = access.readLong(childDeltaFpStart);
        if (parent.childrenDeltaFpBytes < 8) {
          childDeltaFp = childDeltaFp & Node.BYTES_MINUS_1_MASK[parent.childrenDeltaFpBytes - 1];
        }

        long childFp = parent.fp - childDeltaFp;
        assert childFp >= 0 && childFp < parent.fp : "child fp should less than parent fp";
        Node child = Node.load(access, childFp);
        if (matchRemainingBytes(child, target)) {
          return child;
        } else {
          // we get a child, but remaining bytes are different.
          return parent;
        }
      } else {
        return null;
      }
    }
  }

  private boolean matchRemainingBytes(Node node, BytesRef target) {
    if (node.nodeType.equals(NodeType.LEAF_NODE)) {
      if (node.key == null) {
        // If this leaf node has no key, we should scan the suffixes' block.
        return true;
      } else {
        int commonLength =
            ARTUtil.commonPrefixLength(
                target.bytes,
                target.offset,
                target.offset + target.length,
                node.key.bytes,
                node.key.offset,
                node.key.length);
        if (commonLength == node.key.length) {
          target.offset += node.key.length;
          target.length -= node.key.length;
          return true;
        }
      }
    } else {
      if (node.prefixLength > 0) {
        int commonLength =
            ARTUtil.commonPrefixLength(
                target.bytes,
                target.offset,
                target.offset + target.length,
                node.prefix,
                0,
                node.prefixLength);
        if (commonLength == node.prefixLength) {
          target.offset += node.prefixLength;
          target.length -= node.prefixLength;
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }

  /** Find output for the given key(input). Return null if not found. */
  public Output find(BytesRef key) {
    if (key.length == 0) {
      // We may search "";
      if (root.nodeType == NodeType.LEAF_NODE && root.key == null) {
        return root.output;
      } else if (root.prefixLength == 0) {
        return root.output;
      } else {
        return null;
      }
    }
    return find(root, key);
  }

  private Output find(Node node, BytesRef key) {
    while (node != null) {
      if (node.nodeType == NodeType.LEAF_NODE) {
        LeafNode leafNode = (LeafNode) node;
        if (node.key != null && node.key.equals(key)) {
          return leafNode.output;
        }
        return null;
      }
      if (node.prefixLength > 0) {
        int commonLength =
            ARTUtil.commonPrefixLength(
                key.bytes, key.offset, key.offset + key.length, node.prefix, 0, node.prefixLength);
        if (commonLength != node.prefixLength) {
          return null;
        }
        // common prefix is the same, then increase the offset.
        key.offset += node.prefixLength;
        key.length -= node.prefixLength;

        // Work end, match.
        if (key.length == 0) {
          return node.output;
        }
      }

      int pos = node.getChildPos(key.bytes[key.offset]);
      if (pos == Node.ILLEGAL_IDX) {
        return null;
      }
      node = node.getChild(pos);
      key.offset++;
      key.length--;

      // Work end, match.
      if (key.length == 0) {
        return node.output;
      }
    }
    return null;
  }
}
