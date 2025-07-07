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
import org.apache.lucene.util.BytesRef;

public class LeafNode extends Node {
  /**
   * constructor
   *
   * @param key input
   * @param output the corresponding container index
   */
  public LeafNode(BytesRef key, Output output) {
    super(NodeType.LEAF_NODE, 0);
    this.key = key;
    this.output = output;
  }

  @Override
  public int getChildPos(byte k) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Node getChild(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceNode(int pos, Node freshOne) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNextLargerPos(int pos) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxPos() {
    throw new UnsupportedOperationException();
  }

  public void saveChildIndex(IndexOutput dataOutput) throws IOException {
    // empty
    // TODO: save key
  }

  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {
    // empty
  }

  @Override
  void setChildren(Node[] children) {
    throw new UnsupportedOperationException();
  }
}
