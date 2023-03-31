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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import static com.carrotsearch.randomizedtesting.RandomizedTest.frequently;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.util.TestVectorUtil.randomVector;

public class TestNeighborQueue extends LuceneTestCase {

  public void testNeighborsProduct() {
    // make sure we have the sign correct
    NeighborQueue nn = new NeighborQueue(2, false);
    assertTrue(nn.insertWithOverflow(2, 0.5f));
    assertTrue(nn.insertWithOverflow(1, 0.2f));
    assertTrue(nn.insertWithOverflow(3, 1f));
    assertEquals(0.5f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
    nn.pop();
  }

  public void testNeighborsMaxHeap() {
    NeighborQueue nn = new NeighborQueue(2, true);
    assertTrue(nn.insertWithOverflow(2, 2));
    assertTrue(nn.insertWithOverflow(1, 1));
    assertFalse(nn.insertWithOverflow(3, 3));
    assertEquals(2f, nn.topScore(), 0);
    nn.pop();
    assertEquals(1f, nn.topScore(), 0);
  }

  public void testTopMaxHeap() {
    NeighborQueue nn = new NeighborQueue(2, true);
    nn.add(1, 2);
    nn.add(2, 1);
    // lower scores are better; highest score on top
    assertEquals(2, nn.topScore(), 0);
    assertEquals(1, nn.topNode());
  }

  public void testTopMinHeap() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.add(1, 0.5f);
    nn.add(2, -0.5f);
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
    nn.add(1, 1.1f);
    nn.add(2, -2.2f);
    nn.setVisitedCount(42);
    nn.clear();

    assertEquals(0, nn.size());
    assertEquals(0, nn.visitedCount());
  }

  public void testMaxSizeQueue() {
    NeighborQueue nn = new NeighborQueue(2, false);
    nn.add(1, 1);
    nn.add(2, 2);
    assertEquals(2, nn.size());
    assertEquals(1, nn.topNode());

    // insertWithOverflow does not extend the queue
    nn.insertWithOverflow(3, 3);
    assertEquals(2, nn.size());
    assertEquals(2, nn.topNode());

    // add does extend the queue beyond maxSize
    nn.add(4, 1);
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
      nn.add(i, score);
    }
    assertEquals(maxScore, nn.topScore(), 0);
    assertEquals(maxNode, nn.topNode());
  }

  public void testMinHeap_notFull_multiValuedSum() {
    NeighborQueue nn = new NeighborQueue(3, false);

    for (int i = 1; i < 4; i++) {
        for (int j = 0; j < i; j++) {
          nn.insertWithOverflow(i, 0.2f * j, HnswGraphSearcher.Multivalued.SUM);
        }
    }

    HashMap<Integer, Integer> nodeIdToHeapIndex = nn.getNodeIdToHeapIndex();
    assertEquals(3,nodeIdToHeapIndex.size());
    assertEquals(1l, nn.pop());
    assertEquals(2l, nn.pop());
    assertEquals(3l, nn.pop());
  }

  public void testMinHeap_full_multiValuedSum() {
    NeighborQueue nn = new NeighborQueue(3, false);

    for (int i = 1; i < 6; i++) {
      for (int j = 0; j < i; j++) {
        nn.insertWithOverflow(i, 0.2f * j, HnswGraphSearcher.Multivalued.SUM);
      }
    }

    HashMap<Integer, Integer> nodeIdToHeapIndex = nn.getNodeIdToHeapIndex();
    assertEquals(3,nodeIdToHeapIndex.size());
    assertEquals(3l, nn.pop());
    assertEquals(4l, nn.pop());
    assertEquals(5l, nn.pop());
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
        nn.insertWithOverflow(i, score * (j+1), HnswGraphSearcher.Multivalued.MAX);
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
        nn.insertWithOverflow(i, score * (j+1), HnswGraphSearcher.Multivalued.MAX);
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
