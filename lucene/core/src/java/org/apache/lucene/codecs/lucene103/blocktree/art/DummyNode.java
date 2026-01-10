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

  /**
   * get the position of a child corresponding to the input key 'k'
   *
   * @param k a key value of the byte range
   * @return the child position corresponding to the key 'k'
   */
  @Override
  public int getChildPos(byte k) {
    return 0;
  }

  /**
   * get the corresponding key byte of the requested position
   *
   * @param pos the position
   * @return the corresponding key byte
   */
  @Override
  public byte getChildKey(int pos) {
    return 0;
  }

  /**
   * get the child at the specified position in the node, the 'pos' range from 0 to count
   *
   * @param pos the position
   * @return a Node corresponding to the input position
   */
  @Override
  public Node getChild(int pos) {
    return null;
  }

  /**
   * replace the position child to the fresh one
   *
   * @param pos the position
   * @param freshOne the fresh node to replace the old one
   */
  @Override
  public void replaceNode(int pos, Node freshOne) {}

  /**
   * get the next position in the node
   *
   * @param pos current position,-1 to start from the min one
   * @return the next larger byte key's position which is close to 'pos' position,-1 for end
   */
  @Override
  public int getNextLargerPos(int pos) {
    return 0;
  }

  /**
   * get the max child's position
   *
   * @return the max byte key's position
   */
  @Override
  public int getMaxPos() {
    return 0;
  }

  /**
   * Write childIndex to output.
   *
   * @param dataOutput
   */
  @Override
  public void saveChildIndex(IndexOutput dataOutput) throws IOException {}

  /**
   * Write childIndex to output.
   *
   * @param dataInput
   */
  @Override
  public void readChildIndex(IndexInput dataInput) throws IOException {}

  @Override
  public void readChildIndex(RandomAccessInput access, long fp) throws IOException {}

  /**
   * insert the child node into this with the key byte
   *
   * @param childNode the child node
   * @param key the key byte
   * @return the input node4 or an adaptive generated node16
   */
  @Override
  public Node insert(Node childNode, byte key) {
    return null;
  }

  /**
   * Set the node's children to right index while doing the read phase.
   *
   * @param children all the not null children nodes in key byte ascending order, no null element.
   */
  @Override
  void setChildren(Node[] children) {}

  /** Get the node's children. */
  @Override
  Node[] getChildren() {
    return new Node[0];
  }
}
