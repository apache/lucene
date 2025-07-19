package org.apache.lucene.index;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;

public final class NormAndFreqBuffer {

  public long[] norms = LongsRef.EMPTY_LONGS;

  public int[] freqs = IntsRef.EMPTY_INTS;

  public int size;

  public NormAndFreqBuffer() {}

  public void growNoCopy(int minSize) {
    if (size < minSize) {
      norms = ArrayUtil.growNoCopy(norms, minSize);
      freqs = new int[norms.length];
    }
  }
}
