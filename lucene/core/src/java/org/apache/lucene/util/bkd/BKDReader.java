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
package org.apache.lucene.util.bkd;

import java.io.IOException;
import org.apache.lucene.index.PointValues.IntersectVisitor;

/**
 * Abstraction of a block KD-tree that contains multi-dimensional points in byte[] space.
 *
 * @lucene.internal
 */
public interface BKDReader {

  /** BKD tree parameters */
  BKDConfig getConfig();

  /** min packed value */
  byte[] getMinPackedValue();

  /** max packed value */
  byte[] getMaxPackedValue();

  /** Total number of points */
  long getPointCount();

  /** Total number of documents */
  int getDocCount();

  /** Create a new {@link IndexTree} to navigate the index */
  IndexTree getIndexTree() throws IOException;

  /** Basic operations to read the BKD tree. */
  interface IndexTree extends Cloneable {

    /** Clone, the current node becomes the root of the new tree. */
    IndexTree clone();

    /**
     * Move to the first child node and return {@code true} upon success. Returns {@code false} for
     * leaf nodes and {@code true} otherwise. Should not be called if the current node has already
     * called this method.
     */
    boolean moveToChild() throws IOException;

    /**
     * Move to the next sibling node and return {@code true} upon success. Returns {@code false} if
     * the current node has no more siblings.
     */
    boolean moveToSibling() throws IOException;

    /**
     * Move to the parent node and return {@code true} upon success. Returns {@code false} for the
     * root node and {@code true} otherwise.
     */
    boolean moveToParent() throws IOException;

    /** Return the minimum packed value of the current node. */
    byte[] getMinPackedValue();

    /** Return the maximum packed value of the current node. */
    byte[] getMaxPackedValue();

    /** Return the number of points below the current node. */
    long size();

    /** Visit the docs of the current node. Only valid if moveToChild() is false. */
    void visitDocIDs(IntersectVisitor visitor) throws IOException;

    /** Visit the values of the current node. Only valid if moveToChild() is false. */
    void visitDocValues(IntersectVisitor visitor) throws IOException;
  }
}
