package org.apache.lucene.codecs.lucene103;

import static org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat.BLOCK_SIZE;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

public class TestBitSetUtil extends LuceneTestCase {

  public void testDenseBitsetToArray() throws Exception {
    for (int iter = 0; iter < 1000; iter++) {
      FixedBitSet bitSet = new FixedBitSet(BLOCK_SIZE + random().nextInt(200));
      while (bitSet.cardinality() < BLOCK_SIZE) {
        bitSet.set(random().nextInt(bitSet.length()));
      }

      int[] array = new int[BLOCK_SIZE + 1];
      int from, to, size;
      if (random().nextBoolean()) {
        from = 0;
        to = bitSet.length();
      } else {
        from = random().nextInt(bitSet.length());
        to = from + random().nextInt(bitSet.length() - from);
      }

      int base = random().nextInt(1000);
      size = BitSetUtil.denseBitsetToArray(bitSet, from, to, base, array);

      assertEquals(bitSet.cardinality(from, to), size);

      int[] index = new int[] {0};
      bitSet.forEach(from, to, base, bit -> assertEquals(bit, array[index[0]++]));
      assertEquals(size, index[0]);
    }
  }
}
