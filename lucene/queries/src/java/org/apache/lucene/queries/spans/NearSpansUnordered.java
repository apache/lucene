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
package org.apache.lucene.queries.spans;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.util.PriorityQueue;

/**
 * Similar to {@link NearSpansOrdered}, but for the unordered case.
 *
 * <p>Expert: Only public for subclassing. Most implementations should not need this class
 */
public class NearSpansUnordered extends ConjunctionSpans {

  private final int allowedSlop;
  private final PriorityQueue<Spans> spanWindow;

  private int totalSpanLength;
  private int maxEndPosition;

  public NearSpansUnordered(int allowedSlop, List<Spans> subSpans) throws IOException {
    super(subSpans);

    this.allowedSlop = allowedSlop;
    this.spanWindow =
        PriorityQueue.usingComparator(
            super.subSpans.length,
            Comparator.comparingInt(Spans::startPosition).thenComparingInt(Spans::endPosition));
  }

  private void startDocument() throws IOException {
    spanWindow.clear();
    totalSpanLength = 0;
    maxEndPosition = -1;
    for (Spans spans : subSpans) {
      assert spans.startPosition() == -1;
      spans.nextStartPosition();
      assert spans.startPosition() != NO_MORE_POSITIONS;
      spanWindow.add(spans);
      if (spans.endPosition() > maxEndPosition) {
        maxEndPosition = spans.endPosition();
      }
      int spanLength = spans.endPosition() - spans.startPosition();
      assert spanLength >= 0;
      totalSpanLength += spanLength;
    }
  }

  private boolean nextPosition() throws IOException {
    Spans topSpans = spanWindow.top();
    assert topSpans.startPosition() != NO_MORE_POSITIONS;
    int spanLength = topSpans.endPosition() - topSpans.startPosition();
    int nextStartPos = topSpans.nextStartPosition();
    if (nextStartPos == NO_MORE_POSITIONS) {
      return false;
    }
    totalSpanLength -= spanLength;
    spanLength = topSpans.endPosition() - topSpans.startPosition();
    totalSpanLength += spanLength;
    if (topSpans.endPosition() > maxEndPosition) {
      maxEndPosition = topSpans.endPosition();
    }
    spanWindow.updateTop();
    return true;
  }

  private boolean atMatch() {
    return (maxEndPosition - spanWindow.top().startPosition() - totalSpanLength) <= allowedSlop;
  }

  @Override
  boolean twoPhaseCurrentDocMatches() throws IOException {
    // at doc with all subSpans
    startDocument();
    while (true) {
      if (atMatch()) {
        atFirstInCurrentDoc = true;
        oneExhaustedInCurrentDoc = false;
        return true;
      }
      if (!nextPosition()) {
        return false;
      }
    }
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (atFirstInCurrentDoc) {
      atFirstInCurrentDoc = false;
      return spanWindow.top().startPosition();
    }
    assert spanWindow.top().startPosition() != -1;
    assert spanWindow.top().startPosition() != NO_MORE_POSITIONS;
    while (true) {
      if (!nextPosition()) {
        oneExhaustedInCurrentDoc = true;
        return NO_MORE_POSITIONS;
      }
      if (atMatch()) {
        return spanWindow.top().startPosition();
      }
    }
  }

  @Override
  public int startPosition() {
    assert spanWindow.top() != null;
    return atFirstInCurrentDoc
        ? -1
        : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS : spanWindow.top().startPosition();
  }

  @Override
  public int endPosition() {
    return atFirstInCurrentDoc ? -1 : oneExhaustedInCurrentDoc ? NO_MORE_POSITIONS : maxEndPosition;
  }

  @Override
  public int width() {
    return maxEndPosition - spanWindow.top().startPosition();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    for (Spans spans : subSpans) {
      spans.collect(collector);
    }
  }
}
