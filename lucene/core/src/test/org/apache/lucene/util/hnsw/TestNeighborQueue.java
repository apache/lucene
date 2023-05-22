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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.tests.util.LuceneTestCase;

import java.util.HashMap;

public class TestNeighborQueue extends LuceneTestCase {

  public void testNeighborsProduct() {
    // make sure we have the sign correct
    NeighborQueue nn = new NeighborQueue(2, false);
    assertTrue(nn.insertWithOverflow(2, 0.5f, false));
    assertTrue(nn.insertWithOverflow(1, 0.2f, false));
    assertTrue(nn.insertWithOverflow(3, 1f, false));
    assertEquals(0.5f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
    nn.pop();
  }

  public void testNeighborsMaxHeap() {
    NeighborQueue nn = new NeighborQueue(2, true);
    assertTrue(nn.insertWithOverflow(2, 2, false));
    assertTrue(nn.insertWithOverflow(1, 1, false));
    assertFalse(nn.insertWithOverflow(3, 3, false));
    assertEquals(2f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
  }

  public void testTopMaxHeap() {
    NeighborQueue nn = new NeighborQueue(2, true);
    nn.add(1, 2, false);
    nn.add(2, 1, false);
    // lower scores are better; highest score on top
    assertEquals(2, nn.topScore(), 0);
    assertEquals(1, nn.topNode());
  }

  public void testTopMinHeap() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.add(1, 0.5f, false);
    nn.add(2, -0.5f, false);
    // higher scores are better; lowest score on top
    assertEquals(-0.5f, nn.topScore(), 0);
    assertEquals(2, nn.topNode());
  }

  public void testVisitedCount() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.setVisitedCount(100);
    assertEquals(100, nn.visitedCount());
  }

  public void testClear() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.add(1, 1.1f, false);
    nn.add(2, -2.2f, false);
    nn.setVisitedCount(42);
    nn.markIncomplete();
    nn.clear();

    assertEquals(0, nn.size());
    assertEquals(0, nn.visitedCount());
    assertFalse(nn.incomplete());
  }

  public void testMaxSizeQueue() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.add(1, 1, false);
    nn.add(2, 2, false);
    assertEquals(2, nn.size());
    assertEquals(1, nn.topNode());

    // insertWithOverflow does not extend the queue
    nn.insertWithOverflow(3, 3, false);
    assertEquals(2, nn.size());
    assertEquals(2, nn.topNode());

    // add does extend the queue beyond maxSize
    nn.add(4, 1, false);
    assertEquals(3, nn.size());
  }

  public void testUnboundedQueue() {
    NeighborQueue nn = new NeighborQueue(1, true);
    float maxScore = -2;
    int maxNode = -1;
    for (int i = 0; i < 256; i++) {
      // initial size is 32
      float score = random().nextFloat();
      if (score > maxScore) {
        maxScore = score;
        maxNode = i;
      }
      nn.add(i, score, false);
    }
    assertEquals(maxScore, nn.topScore(), 0);
    assertEquals(maxNode, nn.topNode());
  }

  public void testMinHeap_notFull_multiValuedMax() {
    NeighborQueue nn = new NeighborQueue(3, false);

    for (int i = 1; i < 4; i++) {
      for (int j = 0; j < i; j++) {
        float score = 0.2f;
        //node 2 has max score
        if(i==2 && j==0){
          score = 0.9f;
        }
        nn.insertWithOverflow(i, score * (j+1), false);
      }
    }

    HashMap<Integer, Integer> nodeIdToHeapIndex = nn.getNodeIdToHeapIndex();
    assertEquals(3,nodeIdToHeapIndex.size());
    assertEquals(1l, nn.pop());
    assertEquals(3l, nn.pop());
    assertEquals(2l, nn.pop());
  }

  public void testMinHeap_full_multiValuedMax() {
    NeighborQueue nn = new NeighborQueue(3, false);

    for (int i = 1; i < 6; i++) {
      for (int j = 0; j < i; j++) {
        float score = 0.1f;
        //node 2 has max score
        if(i==2 && j==0){
          score = 0.9f;
        }

        if(i==4 && j==0){
          score = 0.8f;
        }
        nn.insertWithOverflow(i, score * (j+1), false);
      }
    }

    HashMap<Integer, Integer> nodeIdToHeapIndex = nn.getNodeIdToHeapIndex();
    assertEquals(3,nodeIdToHeapIndex.size());
    assertEquals(5l, nn.pop());
    assertEquals(4l, nn.pop());
    assertEquals(2l, nn.pop());
  }

  public void testInvalidArguments() {
    expectThrows(IllegalArgumentException.class, () -> new NeighborQueue(0, false));
  }

  public void testToString() {
    assertEquals("Neighbors[0]", new NeighborQueue(2, false).toString());
  }
}
