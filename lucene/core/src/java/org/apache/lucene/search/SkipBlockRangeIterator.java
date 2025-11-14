package org.apache.lucene.search;

import org.apache.lucene.index.DocValuesSkipper;
import java.io.IOException;

public class SkipBlockRangeIterator extends AbstractDocIdSetIterator {

  private final DocValuesSkipper skipper;
  private final long minValue;
  private final long maxValue;

  public SkipBlockRangeIterator(DocValuesSkipper skipper, long minValue, long maxValue) {
    this.skipper = skipper;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    if (target <= skipper.maxDocID(0)) {
      // within current range, and we have already checked the bounds
      return target;
    }
    // Advance to target
    skipper.advance(target);

    // Find the next block in range (could be the current block)
    skipper.advance(minValue, maxValue);

    return doc = Math.max(target, skipper.minDocID(0));
  }

  @Override
  public long cost() {
    return DocIdSetIterator.NO_MORE_DOCS;
  }
}
