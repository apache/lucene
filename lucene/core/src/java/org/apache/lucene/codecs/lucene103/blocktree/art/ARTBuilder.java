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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Builds an ART from pre-sorted terms with outputs. We can save built ART to disk and read it by
 * {@link ARTBuilder}. This version save node, output, floor data's fp, and load node by these fp.
 */
public class ARTBuilder {
  public Node root;

  public ARTBuilder() {
    root = null;
  }

  public boolean isEmpty() {
    return root == null;
  }

  /** 1 Save node's Fp, outputFP, floorDataFp for reader like trie. 2 No recursion */
  public void save(DataOutput meta, IndexOutput index) throws IOException {
    // start FP.
    meta.writeVLong(index.getFilePointer());
    saveNodes(index);
    meta.writeVLong(root.fp);
    index.writeLong(0L); // additional 8 bytes for over-reading (write long N bytes, read long)
    // end FP.
    meta.writeVLong(index.getFilePointer());
  }

  /** No recursion . */
  private void saveNodes(IndexOutput index) throws IOException {
    final long startFP = index.getFilePointer();
    Deque<Node> stack = new ArrayDeque<>();
    stack.push(root);

    // Visit and save nodes of this trie in a post-order depth-first traversal.
    while (stack.isEmpty() == false) {
      Node node = stack.peek();
      assert node.fp == -1;

      if (node.nodeType == NodeType.LEAF_NODE) {
        node.fp = index.getFilePointer() - startFP;
        stack.pop();
        node.saveNode(index);
        continue;
      }

      // If there are any children have not been saved, push the first one into stack and continue.
      // We want to ensure saving children before parent.
      // For node48, its position is the index byte, we save children in index order, so we can
      // calculate child fp when loading.
      if (node.nodeType.equals(NodeType.NODE48)) {
        int nextChildPos = node.savedChildPos + 1;
        if (nextChildPos < node.childrenCount) {
          Node child = node.getChildren()[nextChildPos];
          assert child != null;
          stack.push(child);
          node.savedChildPos = nextChildPos;
          continue;
        }
      } else {
        int nextChildPos = node.getNextLargerPos(node.savedChildPos);

        if (nextChildPos != Node.ILLEGAL_IDX) {
          Node child = node.getChild(nextChildPos);
          assert child != null;
          stack.push(child);
          node.savedChildPos = nextChildPos;
          continue;
        }
      }

      // All children have been written(saved), now it's time to write the parent!
      node.fp = index.getFilePointer() - startFP;
      stack.pop();
      node.saveNode(index);
    }
  }

  /** insert the key and output pair. */
  public void insert(BytesRef key, Output output) {
    Node freshRoot = insert(root, key, 0, output);
    if (freshRoot != root) {
      this.root = freshRoot;
    }
  }

  /** Append an art builder to this. Use ARTBuilder#insert of this method */
  public void append(ARTBuilder artBuilder) {
    Map<BytesRef, Output> kvs = new TreeMap<>();
    visit(artBuilder.root, new BytesRefBuilder(), kvs::put);

    for (var entry : kvs.entrySet()) {
      insert(entry.getKey(), entry.getValue());
    }
  }

  /** Insert an art builder into this one. */
  public void insert(ARTBuilder artBuilder) {
    if (artBuilder.root.nodeType.equals(NodeType.LEAF_NODE)) {
      insert(artBuilder.root.key, artBuilder.root.output);
    } else {
      this.root = insert(this.root, artBuilder.root, 0);
    }
  }

  /**
   * Collect all key, output pairs. Used for tests only. The recursive impl need to be avoided if
   * someone plans to use for production one day.
   */
  void visit(BiConsumer<BytesRef, Output> consumer) {
    visit(root, new BytesRefBuilder(), consumer);
  }

  private void visit(Node node, BytesRefBuilder prefix, BiConsumer<BytesRef, Output> consumer) {
    if (node.output != null) {
      if (node.nodeType == NodeType.LEAF_NODE) {
        if (node.key != null) {
          prefix.append(node.key);
        }
        consumer.accept(prefix.toBytesRef(), node.output);
        return;
      } else {
        if (node.prefixLength > 0) {
          prefix.append(node.prefix, 0, node.prefixLength);
        }
        consumer.accept(prefix.toBytesRef(), node.output);
      }
    }

    int pos = -1;
    while ((pos = node.getNextLargerPos(pos)) != -1) {
      byte indexByte = node.getChildIndexByte(pos);
      Node child = node.getChild(pos);
      // Clone prefix.
      BytesRefBuilder clonedPrefix = new BytesRefBuilder();
      clonedPrefix.copyBytes(prefix);
      clonedPrefix.append(indexByte);

      // We append prefix with output in next visit.
      if (child.output == null && child.prefixLength > 0) {
        clonedPrefix.append(child.prefix, 0, child.prefixLength);
      }
      visit(child, clonedPrefix, consumer);
    }
  }

  /** Set remaining suffix to bytes. */
  private void updateKey(Node node, int from) {
    assert node.key != null;
    assert from > node.key.offset;

    if (from < node.key.offset + node.key.length) {
      // TODO: subtract bytes?
      //      node.key.bytes = ArrayUtil.copyOfSubArray(node.key.bytes, from,
      // node.key.bytes.length);
      node.key.length = node.key.offset + node.key.length - from;
      node.key.offset = from;
    } else {
      node.key = null;
    }
  }

  /** Set remaining suffix to prefix. */
  private void updatePrefix(Node node, int from) {
    assert node.prefix != null;

    if (from < node.prefix.length) {
      node.prefix = ArrayUtil.copyOfSubArray(node.prefix, from, node.prefix.length);
      node.prefixLength = node.prefix.length;
    } else {
      node.prefix = null;
      node.prefixLength = 0;
    }
  }

  private Node insert(Node node, BytesRef key, int depth, Output output) {
    if (node == null) {
      return new LeafNode(key, output);
    }
    if (node.nodeType == NodeType.LEAF_NODE) {
      //      byte[] prefix = leafNode.key.bytes;
      // This happens insert: abc1, abc10, abc100. When inserting abc100 to abc10, there is no key
      // in abc10: abc1 is
      // common prefix, 0 is child index(let it as common prefix for abc10 and abc100, but stay in
      // child index).
      // Or, we even insert a BytesRef("").
      if (node.key == null) {
        Node4 node4 = new Node4(0);
        node4.output = node.output;
        node = null;
        LeafNode anotherLeaf = new LeafNode(key, output);
        assert depth < anotherLeaf.key.length;
        node4.insert(anotherLeaf, key.bytes[depth]);
        updateKey(anotherLeaf, depth + 1);
        // replace the current node with this internal node4
        return node4;
      } else {
        assert key.length > 0;
        assert node.key.offset + node.key.length <= node.key.bytes.length;
        int commonPrefix =
            ARTUtil.commonPrefixLength(
                node.key.bytes,
                depth,
                node.key.offset + node.key.length,
                key.bytes,
                depth,
                key.offset + key.length);
        Node4 node4 = new Node4(commonPrefix);
        // copy common prefix
        node4.prefixLength = commonPrefix;
        if (node4.prefixLength > 0) {
          System.arraycopy(key.bytes, depth, node4.prefix, 0, commonPrefix);
        }
        // generate two leaf nodes as the children of the fresh node4
        // Save output to parent node for node without commonPrefix. e.g. abc1, abc10.

        if (depth + commonPrefix < node.key.offset + node.key.length) {
          node4.insert(node, node.key.bytes[depth + commonPrefix]);
          updateKey(node, depth + commonPrefix + 1);
        } else {
          node4.output = node.output;
          node = null;
        }
        LeafNode anotherLeaf = new LeafNode(key, output);
        assert depth + commonPrefix < anotherLeaf.key.length;
        node4.insert(anotherLeaf, key.bytes[depth + commonPrefix]);
        updateKey(anotherLeaf, depth + commonPrefix + 1);
        // replace the current node with this internal node4
        return node4;
      }
    }
    // to a inner node case
    if (node.prefixLength > 0) {
      // find the mismatch position
      int mismatchPos =
          Arrays.mismatch(node.prefix, 0, node.prefixLength, key.bytes, depth, key.length);
      if (mismatchPos != node.prefixLength) {
        Node4 node4 = new Node4(mismatchPos);
        // copy prefix
        node4.prefixLength = mismatchPos;
        if (node4.prefixLength > 0) {
          System.arraycopy(node.prefix, 0, node4.prefix, 0, mismatchPos);
        }
        // split the current internal node, spawn a fresh node4 and let the
        // current internal node as its children.
        node4.insert(node, node.prefix[mismatchPos]);
        updatePrefix(node, mismatchPos + 1);
        LeafNode leafNode = new LeafNode(key, output);
        node4.insert(leafNode, key.bytes[mismatchPos + depth]);
        updateKey(leafNode, mismatchPos + depth + 1);
        return node4;
      }
      depth += node.prefixLength;
    }
    int pos = node.getChildPos(key.bytes[depth]);
    if (pos != Node.ILLEGAL_IDX) {
      // insert the key as current internal node's children's child node.
      Node child = node.getChild(pos);
      Node freshOne = insert(child, key, depth + 1, output);
      if (freshOne != child) {
        node.replaceNode(pos, freshOne);
      }
      return node;
    }
    // insert the key as a child leaf node of the current internal node
    LeafNode leafNode = new LeafNode(key, output);
    Node freshOne = node.insert(leafNode, key.bytes[depth]);
    updateKey(leafNode, depth + 1);
    return freshOne;
  }

  private Node insert(Node node, Node child, int depth) {
    assert child.nodeType.equals(NodeType.LEAF_NODE) == false
        : "Use insert k,v method to insert leaf node";

    if (node.nodeType == NodeType.LEAF_NODE) {
      if (node.key == null) {
        Node4 node4 = new Node4(0);
        node4.output = node.output;
        node = null;
        assert depth < child.prefixLength;
        node4.insert(child, child.prefix[depth]);
        updatePrefix(child, depth + 1);
        // replace the current node with this internal node4
        return node4;
      } else {
        int commonPrefix =
            ARTUtil.commonPrefixLength(
                node.key.bytes,
                depth,
                node.key.offset + node.key.length,
                child.prefix,
                depth,
                child.prefixLength);
        Node4 node4 = new Node4(commonPrefix);
        // copy common prefix
        node4.prefixLength = commonPrefix;
        if (node4.prefixLength > 0) {
          System.arraycopy(node.key.bytes, depth, node4.prefix, 0, commonPrefix);
        }
        // generate two leaf nodes as the children of the fresh node4
        // Save output to parent node for node without commonPrefix. e.g. abc1, abc10.

        if (depth + commonPrefix < node.key.offset + node.key.length) {
          node4.insert(node, node.key.bytes[depth + commonPrefix]);
          updateKey(node, depth + commonPrefix + 1);
        } else {
          node4.output = node.output;
          node = null;
        }

        assert depth + commonPrefix < child.prefixLength;
        node4.insert(child, child.prefix[depth + commonPrefix]);
        updatePrefix(child, depth + commonPrefix + 1);
        // replace the current node with this internal node4
        return node4;
      }
    }
    // to a inner node case
    if (node.prefixLength > 0) {
      // find the mismatch position
      int mismatchPos =
          Arrays.mismatch(
              node.prefix, 0, node.prefixLength, child.prefix, depth, child.prefixLength);
      if (mismatchPos != node.prefixLength) {
        Node4 node4 = new Node4(mismatchPos);
        // copy prefix
        node4.prefixLength = mismatchPos;
        if (node4.prefixLength > 0) {
          System.arraycopy(node.prefix, 0, node4.prefix, 0, mismatchPos);
        }
        // split the current internal node, spawn a fresh node4 and let the
        // current internal node as its children.
        node4.insert(node, node.prefix[mismatchPos]);
        updatePrefix(node, mismatchPos + 1);

        node4.insert(child, child.prefix[mismatchPos + depth]);
        updatePrefix(child, mismatchPos + depth + 1);
        return node4;
      }
      depth += node.prefixLength;
    }
    int pos = node.getChildPos(child.prefix[depth]);
    if (pos != Node.ILLEGAL_IDX) {
      // insert the key as current internal node's children's child node.
      Node oldChild = node.getChild(pos);
      Node newChild = insert(oldChild, child, depth + 1);
      if (newChild != oldChild) {
        node.replaceNode(pos, newChild);
      }
      return node;
    }
    // insert the key as a child leaf node of the current internal node
    Node freshOne = node.insert(child, child.prefix[depth]);
    updatePrefix(child, depth + 1);
    return freshOne;
  }
}
