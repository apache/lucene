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

package org.apache.lucene.sandbox.codecs.lucene90.randomaccess;

import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;

abstract class TermStateCodecComponent {

  static byte getBitWidth(IntBlockTermState[] termStates, TermStateCodecComponent component) {
    assert termStates.length > 0;

    long maxValSeen = -1;
    long referenceValue =
        component.isMonotonicallyIncreasing() ? component.getTargetValue(termStates[0]) : 0;

    for (var termState : termStates) {
      maxValSeen = Math.max(maxValSeen, component.getTargetValue(termState) - referenceValue);
    }
    return (byte) (64 - Long.numberOfLeadingZeros(maxValSeen));
  }

  abstract boolean isMonotonicallyIncreasing();

  abstract long getTargetValue(IntBlockTermState termState);

  abstract void setTargetValue(IntBlockTermState termState, long value);

  /** Below are the relevant IntBlockTermState components * */
  static final class SingletonDocId extends TermStateCodecComponent {
    public static SingletonDocId INSTANCE = new SingletonDocId();

    private SingletonDocId() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.singletonDocID;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      assert value <= Integer.MAX_VALUE;
      // A correct codec implementation does not change the value,
      // after the encode/decode round-trip it should still be a valid int
      termState.singletonDocID = (int) value;
    }
  }

  static final class DocFreq extends TermStateCodecComponent {
    public static DocFreq INSTANCE = new DocFreq();

    private DocFreq() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.docFreq;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      assert value <= Integer.MAX_VALUE;
      // A correct codec implementation does not change the value,
      // after the encode/decode round-trip it should still be a valid int
      termState.docFreq = (int) value;
    }
  }

  static final class TotalTermFreq extends TermStateCodecComponent {
    public static TotalTermFreq INSTANCE = new TotalTermFreq();

    private TotalTermFreq() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.totalTermFreq;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.totalTermFreq = value;
    }
  }

  static final class DocStartFP extends TermStateCodecComponent {
    public static DocStartFP INSTANCE = new DocStartFP();

    private DocStartFP() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return true;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.docStartFP;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.docStartFP = value;
    }
  }

  static final class PositionStartFP extends TermStateCodecComponent {
    public static PositionStartFP INSTANCE = new PositionStartFP();

    private PositionStartFP() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return true;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.posStartFP;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.posStartFP = value;
    }
  }

  static final class PayloadStartFP extends TermStateCodecComponent {
    public static PayloadStartFP INSTANCE = new PayloadStartFP();

    private PayloadStartFP() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return true;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.payStartFP;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.payStartFP = value;
    }
  }

  static final class SkipOffset extends TermStateCodecComponent {
    public static SkipOffset INSTANCE = new SkipOffset();

    private SkipOffset() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.skipOffset;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.skipOffset = value;
    }
  }

  static final class LastPositionBlockOffset extends TermStateCodecComponent {
    public static LastPositionBlockOffset INSTANCE = new LastPositionBlockOffset();

    private LastPositionBlockOffset() {}

    @Override
    public boolean isMonotonicallyIncreasing() {
      return false;
    }

    @Override
    public long getTargetValue(IntBlockTermState termState) {
      return termState.lastPosBlockOffset;
    }

    @Override
    public void setTargetValue(IntBlockTermState termState, long value) {
      termState.lastPosBlockOffset = value;
    }
  }
}
