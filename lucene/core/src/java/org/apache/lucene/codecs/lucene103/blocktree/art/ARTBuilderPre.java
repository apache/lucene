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
import java.util.Arrays;
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
 * {@link ARTBuilderPre}. This version save/load the whole art one time, without FP. This can be
 * used in NormalizeCharMap.
 */
public class ARTBuilderPre {
  public Node root;

  public ARTBuilderPre() {
    root = null;
  }

  public boolean isEmpty() {
    return root == null;
  }

  public void save(DataOutput meta, IndexOutput data) throws IOException {
    // start FP.
    meta.writeVLong(data.getFilePointer());
    save(root, data);
    // end FP.
    meta.writeVLong(data.getFilePointer());
  }

  private void save(Node node, IndexOutput data) throws IOException {
    if (node.nodeType != NodeType.LEAF_NODE) {
      // Save node.
      node.save(data);
      // Save children.
      int nextPos = node.getNextLargerPos(Node.ILLEGAL_IDX);
      while (nextPos != Node.ILLEGAL_IDX) {
        Node child = node.getChild(nextPos);
        assert child != null;
        // TODO: Use stack to eliminate this recursion.
        save(child, data);
        nextPos = node.getNextLargerPos(nextPos);
      }
    } else {
      // Save leaf Node
      node.save(data);
    }
  }

  /** insert the key and output pair. */
  public void insert(BytesRef key, Output output) {
    Node freshRoot = insert(root, key, 0, output);
    if (freshRoot != root) {
      this.root = freshRoot;
    }
  }

  /** Append an art builder to this. */
  public void append(ARTBuilderPre artBuilder) {
    // TODO: Improve this , or store kvs instead of tmp compiled art in PendingBlock#subIndices, and
    // compile it once in/before save ?.
    Map<BytesRef, Output> kvs = new TreeMap<>();
    visit(artBuilder.root, new BytesRefBuilder(), kvs::put);

    for (var entry : kvs.entrySet()) {
      insert(entry.getKey(), entry.getValue());
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
      byte key = node.getChildKey(pos);
      Node child = node.getChild(pos);
      // Clone prefix.
      BytesRefBuilder clonedPrefix = new BytesRefBuilder();
      clonedPrefix.copyBytes(prefix);
      clonedPrefix.append(key);

      // We append prefix with output in next visit.
      if (child.output == null && child.prefixLength > 0) {
        clonedPrefix.append(child.prefix, 0, child.prefixLength);
      }
      visit(child, clonedPrefix, consumer);
    }
  }

  /** Set remaining suffix to bytes. */
  private void updateNodeBytes(Node node, int from) {
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
  private void updateNodePrefix(Node node, int from) {
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
      LeafNode leafNode = (LeafNode) node;
      //      byte[] prefix = leafNode.key.bytes;
      // This happens insert: abc1, abc10, abc100. When inserting abc100 to abc10, there is no key
      // in abc10: abc1 is
      // common prefix, 0 is child index(let it as common prefix for abc10 and abc100, but stay in
      // child index).
      // Or, we even insert a BytesRef("").
      if (leafNode.key == null) {
        Node4 node4 = new Node4(0);
        node4.output = leafNode.output;
        leafNode = null;
        LeafNode anotherLeaf = new LeafNode(key, output);
        assert depth < anotherLeaf.key.length;
        Node4.insert(node4, anotherLeaf, key.bytes[depth]);
        updateNodeBytes(anotherLeaf, depth + 1);
        // replace the current node with this internal node4
        return node4;
      } else {
        assert key.length > 0;
        assert leafNode.key.offset + leafNode.key.length <= leafNode.key.bytes.length;
        byte[] prefix = leafNode.key.bytes;
        int commonPrefix =
            ARTUtil.commonPrefixLength(
                prefix,
                depth,
                leafNode.key.offset + leafNode.key.length,
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

        if (depth + commonPrefix < leafNode.key.offset + leafNode.key.length) {
          Node4.insert(node4, leafNode, prefix[depth + commonPrefix]);
          updateNodeBytes(leafNode, depth + commonPrefix + 1);
        } else {
          node4.output = leafNode.output;
          leafNode = null;
        }
        LeafNode anotherLeaf = new LeafNode(key, output);
        assert depth + commonPrefix < anotherLeaf.key.length;
        Node4.insert(node4, anotherLeaf, key.bytes[depth + commonPrefix]);
        updateNodeBytes(anotherLeaf, depth + commonPrefix + 1);
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
        Node4.insert(node4, node, node.prefix[mismatchPos]);
        updateNodePrefix(node, mismatchPos + 1);
        LeafNode leafNode = new LeafNode(key, output);
        Node4.insert(node4, leafNode, key.bytes[mismatchPos + depth]);
        updateNodeBytes(leafNode, mismatchPos + depth + 1);
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
    Node freshOne = Node.insertLeaf(node, leafNode, key.bytes[depth]);
    updateNodeBytes(leafNode, depth + 1);
    return freshOne;
  }
}
