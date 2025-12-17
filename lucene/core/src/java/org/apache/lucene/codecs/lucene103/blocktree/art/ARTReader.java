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

import static org.apache.lucene.codecs.lucene103.blocktree.art.Node.BYTES_MINUS_1_MASK;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

/**
 * Visit or find(search) terms. We can read an ART from disk, or from root node directly(similar to
 * the usage of fst in org.apache.lucene.analysis.charfilter.NormalizeCharMap).
 */
public class ARTReader {

  final RandomAccessInput access;
  final IndexInput input;
  private Node root;

  // For testing.
  public Node getRoot() {
    return root;
  }

  // For testing.
  public ARTReader(Node root) {
    this.access = null;
    this.input = null;
    this.root = root;
  }

  public ARTReader(IndexInput input) throws IOException {
    this.access = null;
    this.input = input;
    this.root = read(input);
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

  // Read whole art into heap.
  private Node read(IndexInput dataInput) throws IOException {
    // TODO: Read specify node by node's fp like trie.
    Node node = Node.read(dataInput);

    if (node.nodeType == NodeType.LEAF_NODE) {
      return node;
    } else {
      // Children count.
      Node[] children = new Node[node.childrenCount];
      // Read all not null children.
      //      System.out.println(node);
      for (int i = 0; i < node.childrenCount; i++) {
        Node child = read(dataInput);
        children[i] = child;
      }
      node.setChildren(children);
      return node;
    }
  }

  // TODO: read/set childrenFps. then we find pos, get fet pos' fp.
  private Node read2(IndexInput dataInput) throws IOException {
    // TODO: Read specify node by node's fp like trie.
    Node node = Node.read(dataInput);

    if (node.nodeType == NodeType.LEAF_NODE) {
      return node;
    } else {
      // Children count.
      Node[] children = new Node[node.childrenCount];
      // Read all not null children.
      //      System.out.println(node);
      for (int i = 0; i < node.childrenCount; i++) {
        Node child = read(dataInput);
        children[i] = child;
      }
      node.setChildren(children);
      return node;
    }
  }

  /**
   * Find the next node from a cached (searched) one, this is useful when seeking in
   * SegmentTermsEnum.
   */
  // TODO: Assign child if need.
  public Node findTargetNode(Node node, BytesRef target, Node child) {
    assert node != null;

    // TODO: Target length is 0 may never happen, when we search step by step?
    if (target.length == 0) {
      // We may search "";
      if (node.nodeType == NodeType.LEAF_NODE && node.key == null) {
        // Match, Use this node's output.
        return null;
      } else if (node.prefixLength == 0) {
        // Match, Use this node's output.
        return null;
      } else {
        // Not match, keep in this node.
        return node;
      }
    }

    if (node.nodeType.equals(NodeType.LEAF_NODE)) {
      if (node.key.equals(target)) {
        // Match, Use this node's output.
        return null;
      } else {
        // Not match, keep in this node.
        return node;
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
        if (commonLength != node.prefixLength) {
          // Not match, keep in this node.
          return node;
        }
        // common prefix is the same, then increase the offset.
        target.offset += node.prefixLength;
        target.length -= node.prefixLength;
        // Work end, match.
        if (target.length == 0) {
          // Match, Use this node's output.
          return null;
        }
      }

      // Get child.
      int childPos = node.getChildPos(target.bytes[target.offset]);
      target.offset++;
      target.length--;
      if (childPos != Node.ILLEGAL_IDX) {
        return node.getChild(childPos);
      } else {
        // Not match, keep in this node.
        return node;
      }
    }
  }

  /**
   * Find the next parent from a cached (searched) one, this is useful when seeking in
   * SegmentTermsEnum.
   */
  public Node lookupChild(BytesRef target, Node parent, Node child) throws IOException {
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
      byte key = target.bytes[target.offset];
      int childPos = parent.getChildPos(target.bytes[target.offset]);
      target.offset++;
      target.length--;
      if (childPos != Node.ILLEGAL_IDX) {
        // For Node 256, there is gap in children, we need minus the number of null child from 0 to
        // this pos.
        // For Node 48, childPos is the key byte, we need use the read index in children.
        long childDeltaFpStart;
        if (parent.nodeType.equals(NodeType.NODE48)) {
          int childIndex = ((Node48) parent).getChildIndex(key);
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
          childDeltaFp = childDeltaFp & BYTES_MINUS_1_MASK[parent.childrenDeltaFpBytes - 1];
        }

        long childFp = parent.fp - childDeltaFp;
        assert childFp > 0 && childFp < parent.fp : "child fp should less than parent fp";
        return Node.load(access, childFp);
      } else {
        // Not match, keep in this parent.
        return parent;
      }
    }
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
