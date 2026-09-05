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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

/** An ART node used for searching and overwriting. */
public class DummyNode extends Node {

  /** constructor */
  public DummyNode() {
    super(NodeType.DUMMY_NODE, 0);
  }

  /** Get the position of a child corresponding to the input index byte. */
  @Override
  public int getChildPos(byte indexByte) {
    return 0;
  }

  /** Get the corresponding index byte of the requested position. */
  @Override
  public byte getChildIndexByte(int pos) {
    return 0;
  }

  /** Get the child at the specified position in the node, the 'pos' range from 0 to count. */
  @Override
  public Node getChild(int pos) {
    return null;
  }

  /** Replace the position child to the fresh one. */
  @Override
  public void replaceNode(int pos, Node freshOne) {}

  /** Get the next position in the node. */
  @Override
  public int getNextLargerPos(int pos) {
    return 0;
  }

  /** Get the max child's position. */
  @Override
  public int getMaxPos() {
    return 0;
  }

  /** Write childIndex to output. */
  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {}

  /** Write childIndex to output. */
  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {}

  @Override
  public int readChildIndex(RandomAccessInput access, long fp) throws IOException {
    return 0;
  }

  /** Insert the child node into this with the index byte. */
  @Override
  public Node insert(Node childNode, byte indexByte) {
    return null;
  }

  /** Set the node's children to right index while doing the read phase. */
  @Override
  void setChildren(Node[] children) {}

  /** Get the node's children. */
  @Override
  Node[] getChildren() {
    return new Node[0];
  }
}
