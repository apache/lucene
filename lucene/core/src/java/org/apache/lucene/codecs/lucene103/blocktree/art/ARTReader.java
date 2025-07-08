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
import org.apache.lucene.util.BytesRef;

public class ARTReader {
  private Node root;

  // For testing.
  public Node getRoot() {
    return root;
  }

  // For testing.
  public ARTReader(Node root) {
    this.root = root;
  }

  public ARTReader(IndexInput dataInput) throws IOException {
    this.root = read(dataInput);
  }

  public Node read(IndexInput dataInput) throws IOException {
    // TODO: Read specify node by node's fp like trie.
    Node node = Node.read(dataInput);

    if (node.nodeType == NodeType.LEAF_NODE) {
      return node;
    } else {
      // Children count.
      Node[] children = new Node[node.count];
      // Read all not null children.
      //      System.out.println(node);
      for (int i = 0; i < node.count; i++) {
        Node child = read(dataInput);
        children[i] = child;
      }
      node.setChildren(children);
      return node;
    }
  }

  public Output find(BytesRef key) {
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
