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
package org.apache.lucene.codecs.bloom;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A class used to represent a set of many, potentially large, values (e.g. many long strings such
 * as URLs), using a significantly smaller amount of memory.
 *
 * <p>The set is "lossy" in that it cannot definitively state that is does contain a value but it
 * <em>can</em> definitively say if a value is <em>not</em> in the set. It can therefore be used as
 * a Bloom Filter. Another application of the set is that it can be used to perform fuzzy counting
 * because it can estimate reasonably accurately how many unique values are contained in the set.
 *
 * <p>This class is NOT threadsafe.
 *
 * <p>Internally a Bitset is used to record values and once a client has finished recording a stream
 * of values the {@link #downsize(float)} method can be used to create a suitably smaller set that
 * is sized appropriately for the number of values recorded and desired saturation levels.
 *
 * @lucene.experimental
 */
public class FuzzySet implements Accountable {

  /**
   * Result from {@link FuzzySet#contains(BytesRef)}: can never return definitively YES (always
   * MAYBE), but can sometimes definitely return NO.
   */
  public enum ContainsResult {
    MAYBE,
    NO
  };

  private HashFunction hashFunction;
  private FixedBitSet filter;
  private int bloomSize;
  private final int hashCount;

  // The sizes of BitSet used are all numbers that, when expressed in binary form,
  // are all ones. This is to enable fast downsizing from one bitset to another
  // by simply ANDing each set index in one bitset with the size of the target bitset
  // - this provides a fast modulo of the number. Values previously accumulated in
  // a large bitset and then mapped to a smaller set can be looked up using a single
  // AND operation of the query term's hash rather than needing to perform a 2-step
  // translation of the query term that mirrors the stored content's reprojections.
  static final int[] usableBitSetSizes;

  static {
    usableBitSetSizes = new int[26];
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      usableBitSetSizes[i] = (1 << (i + 6)) - 1;
    }
  }

  /**
   * Rounds down required maxNumberOfBits to the nearest number that is made up of all ones as a
   * binary number. Use this method where controlling memory use is paramount.
   */
  public static int getNearestSetSize(int maxNumberOfBits) {
    int result = usableBitSetSizes[0];
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      if (usableBitSetSizes[i] <= maxNumberOfBits) {
        result = usableBitSetSizes[i];
      }
    }
    return result;
  }

  /**
   * Use this method to choose a set size where accuracy (low content saturation) is more important
   * than deciding how much memory to throw at the problem.
   *
   * @param desiredSaturation A number between 0 and 1 expressing the % of bits set once all values
   *     have been recorded
   * @return The size of the set nearest to the required size
   */
  public static int getNearestSetSize(int maxNumberOfValuesExpected, float desiredSaturation) {
    // Iterate around the various scales of bitset from smallest to largest looking for the first
    // that
    // satisfies value volumes at the chosen saturation level
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      int numSetBitsAtDesiredSaturation = (int) (usableBitSetSizes[i] * desiredSaturation);
      int estimatedNumUniqueValues =
          getEstimatedNumberUniqueValuesAllowingForCollisions(
              usableBitSetSizes[i], numSetBitsAtDesiredSaturation);
      if (estimatedNumUniqueValues > maxNumberOfValuesExpected) {
        return usableBitSetSizes[i];
      }
    }
    return -1;
  }

  public static FuzzySet createSetBasedOnMaxMemory(int maxNumBytes) {
    int setSize = getNearestSetSize(maxNumBytes);
    return new FuzzySet(new FixedBitSet(setSize + 1), setSize, 1);
  }

  public static FuzzySet createSetBasedOnQuality(
      int maxNumUniqueValues, float desiredMaxSaturation, int version) {
    int setSize = getNearestSetSize(maxNumUniqueValues, desiredMaxSaturation);
    return new FuzzySet(new FixedBitSet(setSize + 1), setSize, 1);
  }

  public static FuzzySet createOptimalSet(int maxNumUniqueValues, float targetMaxFpp) {
    int setSize =
        (int)
            Math.ceil(
                (maxNumUniqueValues * Math.log(targetMaxFpp))
                    / Math.log(1 / Math.pow(2, Math.log(2))));
    setSize = getNearestSetSize(2 * setSize);
    int optimalK = (int) Math.round(((double) setSize / maxNumUniqueValues) * Math.log(2));
    return new FuzzySet(new FixedBitSet(setSize + 1), setSize, optimalK);
  }

  private FuzzySet(FixedBitSet filter, int bloomSize, int hashCount) {
    super();
    this.filter = filter;
    this.bloomSize = bloomSize;
    this.hashFunction = MurmurHash64.INSTANCE;
    this.hashCount = hashCount;
  }

  /**
   * The main method required for a Bloom filter which, given a value determines set membership.
   * Unlike a conventional set, the fuzzy set returns NO or MAYBE rather than true or false. Hash
   * generation follows the same principles as {@link #addValue(BytesRef)}
   *
   * @return NO or MAYBE
   */
  public ContainsResult contains(BytesRef value) {
    long hash = hashFunction.hash(value);
    int msb = (int) (hash >>> Integer.SIZE);
    int lsb = (int) hash;
    for (int i = 0; i < hashCount; i++) {
      int bloomPos = (lsb + i * msb);
      if (!mayContainValue(bloomPos)) {
        return ContainsResult.NO;
      }
    }
    return ContainsResult.MAYBE;
  }

  /**
   * Serializes the data set to file using the following format:
   *
   * <ul>
   *   <li>FuzzySet --&gt;hashCount,BloomSize, NumBitSetWords,BitSetWord<sup>NumBitSetWords</sup>
   *   <li>hashCount --&gt; {@link DataOutput#writeVInt Uint32} The number of hash functions (k).
   *   <li>BloomSize --&gt; {@link DataOutput#writeInt Uint32} The modulo value used to project
   *       hashes into the field's Bitset
   *   <li>NumBitSetWords --&gt; {@link DataOutput#writeInt Uint32} The number of longs (as returned
   *       from {@link FixedBitSet#getBits})
   *   <li>BitSetWord --&gt; {@link DataOutput#writeLong Long} A long from the array returned by
   *       {@link FixedBitSet#getBits}
   * </ul>
   *
   * @param out Data output stream
   * @throws IOException If there is a low-level I/O error
   */
  public void serialize(DataOutput out) throws IOException {
    out.writeVInt(hashCount);
    out.writeInt(bloomSize);
    long[] bits = filter.getBits();
    out.writeInt(bits.length);
    for (int i = 0; i < bits.length; i++) {
      // Can't used VLong encoding because cant cope with negative numbers
      // output by FixedBitSet
      out.writeLong(bits[i]);
    }
  }

  public static FuzzySet deserialize(DataInput in) throws IOException {
    int hashCount = in.readVInt();
    int bloomSize = in.readInt();
    int numLongs = in.readInt();
    long[] longs = new long[numLongs];
    for (int i = 0; i < numLongs; i++) {
      longs[i] = in.readLong();
    }
    FixedBitSet bits = new FixedBitSet(longs, bloomSize + 1);
    return new FuzzySet(bits, bloomSize, hashCount);
  }

  private boolean mayContainValue(int aHash) {
    // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
    int pos = aHash & bloomSize;
    return filter.get(pos);
  }

  /**
   * Records a value in the set. The referenced bytes are hashed. From the 64-bit generated hash,
   * two 32-bit hashes are derived from the msb and lsb which can be used to derive more hashes (see
   * https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf). Finally, each generated hash
   * is modulo n'd where n is the chosen size of the internal bitset.
   *
   * @param value the key value to be hashed
   * @throws IOException If there is a low-level I/O error
   */
  public void addValue(BytesRef value) throws IOException {
    long hash = hashFunction.hash(value);
    int msb = (int) (hash >>> Integer.SIZE);
    int lsb = (int) hash;
    for (int i = 0; i < hashCount; i++) {
      // Bitmasking using bloomSize is effectively a modulo operation.
      int bloomPos = (lsb + i * msb) & bloomSize;
      filter.set(bloomPos);
    }
  }

  /**
   * @param targetMaxSaturation A number between 0 and 1 describing the % of bits that would ideally
   *     be set in the result. Lower values have better accuracy but require more space.
   * @return a smaller FuzzySet or null if the current set is already over-saturated
   */
  public FuzzySet downsize(float targetMaxSaturation) {
    int numBitsSet = filter.cardinality();
    FixedBitSet rightSizedBitSet = filter;
    int rightSizedBitSetSize = bloomSize;
    // Hopefully find a smaller size bitset into which we can project accumulated values while
    // maintaining desired saturation level
    for (int i = 0; i < usableBitSetSizes.length; i++) {
      int candidateBitsetSize = usableBitSetSizes[i];
      float candidateSaturation = (float) numBitsSet / (float) candidateBitsetSize;
      if (candidateSaturation <= targetMaxSaturation) {
        rightSizedBitSetSize = candidateBitsetSize;
        break;
      }
    }
    // Re-project the numbers to a smaller space if necessary
    if (rightSizedBitSetSize < bloomSize) {
      // Reset the choice of bitset to the smaller version
      rightSizedBitSet = new FixedBitSet(rightSizedBitSetSize + 1);
      // Map across the bits from the large set to the smaller one
      int bitIndex = 0;
      do {
        bitIndex = filter.nextSetBit(bitIndex);
        if (bitIndex != DocIdSetIterator.NO_MORE_DOCS) {
          // Project the larger number into a smaller one effectively
          // modulo-ing by using the target bitset size as a mask
          int downSizedBitIndex = bitIndex & rightSizedBitSetSize;
          rightSizedBitSet.set(downSizedBitIndex);
          bitIndex++;
        }
      } while ((bitIndex >= 0) && (bitIndex <= bloomSize));
    } else {
      return null;
    }
    return new FuzzySet(rightSizedBitSet, rightSizedBitSetSize, hashCount);
  }

  public int getEstimatedUniqueValues() {
    return getEstimatedNumberUniqueValuesAllowingForCollisions(bloomSize, filter.cardinality());
  }

  // Given a set size and a the number of set bits, produces an estimate of the number of unique
  // values recorded
  public static int getEstimatedNumberUniqueValuesAllowingForCollisions(
      int setSize, int numRecordedBits) {
    double setSizeAsDouble = setSize;
    double numRecordedBitsAsDouble = numRecordedBits;
    double saturation = numRecordedBitsAsDouble / setSizeAsDouble;
    double logInverseSaturation = Math.log(1 - saturation) * -1;
    return (int) (setSizeAsDouble * logInverseSaturation);
  }

  public float getTargetMaxSaturation() {
    return 0.5f;
  }

  public float getSaturation() {
    int numBitsSet = filter.cardinality();
    return (float) numBitsSet / (float) bloomSize;
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(filter.getBits());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(hash="
        + hashFunction
        + ", k="
        + hashCount
        + ", bits="
        + filter.cardinality()
        + "/"
        + filter.length()
        + ")";
  }
}
