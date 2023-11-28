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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunnable;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.fst.BytesRefPrimitiveLongFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PrimitiveLongFST;
import org.apache.lucene.util.fst.PrimitiveLongFST.PrimitiveLongArc;
import org.apache.lucene.util.fst.PrimitiveLongFST.PrimitiveLongFSTOutputs;
import org.apache.lucene.util.fst.Util;

final class TermsImpl extends Terms {
  private final FieldInfo fieldInfo;

  private final RandomAccessTermsDict termsDict;

  private final Lucene99PostingsReader lucene99PostingsReader;

  public TermsImpl(
      FieldInfo fieldInfo,
      RandomAccessTermsDict termsDict,
      Lucene99PostingsReader lucene99PostingsReader) {
    this.fieldInfo = fieldInfo;
    this.termsDict = termsDict;
    this.lucene99PostingsReader = lucene99PostingsReader;
  }

  @Override
  public long size() throws IOException {
    return termsDict.termsStats().size();
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
    return termsDict.termsStats().sumTotalTermFreq();
  }

  @Override
  public long getSumDocFreq() throws IOException {
    return termsDict.termsStats().sumDocFreq();
  }

  @Override
  public int getDocCount() throws IOException {
    return termsDict.termsStats().docCount();
  }

  @Override
  public boolean hasFreqs() {
    return fieldInfo.getIndexOptions().ordinal() >= IndexOptions.DOCS_AND_FREQS.ordinal();
  }

  @Override
  public boolean hasOffsets() {
    return fieldInfo.getIndexOptions().ordinal()
        >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS.ordinal();
  }

  @Override
  public boolean hasPositions() {
    return fieldInfo.getIndexOptions().ordinal()
        >= IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal();
  }

  @Override
  public boolean hasPayloads() {
    return fieldInfo.hasPayloads();
  }

  @Override
  public BytesRef getMin() throws IOException {
    return termsDict.termsStats().minTerm();
  }

  @Override
  public BytesRef getMax() throws IOException {
    return termsDict.termsStats().maxTerm();
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new RandomAccessTermsEnum();
  }

  @Override
  public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
    if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
      throw new IllegalArgumentException("please use CompiledAutomaton.getTermsEnum instead");
    }
    return new RandomAccessIntersectTermsEnum(compiled, startTerm);
  }

  final class RandomAccessTermsEnum extends TermsEnum {
    private AttributeSource attrs;

    private BytesRef term;

    private boolean isTermStateCurrent;

    private IntBlockTermState termState;

    private final BytesRefPrimitiveLongFSTEnum fstEnum;

    private BytesRefPrimitiveLongFSTEnum.InputOutput fstSeekState;

    // Only set when seekExact(term, state) is called, because that will update
    // the termState but leave the fstSeekState out of sync.
    // We need to re-seek in next() calls to catch up to that term.
    private boolean needReSeekInNext;

    private final TermDataReaderProvider.TermDataReader termDataReader;

    RandomAccessTermsEnum() throws IOException {
      termState = (IntBlockTermState) lucene99PostingsReader.newTermState();
      fstEnum = new BytesRefPrimitiveLongFSTEnum(termsDict.termsIndex().primitiveLongFST());
      termDataReader = termsDict.termDataReaderProvider().newReader();
    }

    void updateTermStateIfNeeded() throws IOException {
      if (!isTermStateCurrent && !needReSeekInNext) {
        TermsIndex.TypeAndOrd typeAndOrd = TermsIndex.decodeLong(fstSeekState.output);
        termState =
            termDataReader.getTermState(
                typeAndOrd.termType(), typeAndOrd.ord(), fieldInfo.getIndexOptions());
        isTermStateCurrent = true;
      }
    }

    @Override
    public AttributeSource attributes() {
      if (attrs == null) {
        attrs = new AttributeSource();
      }
      return attrs;
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
      fstSeekState = fstEnum.seekExact(text);
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      return term != null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      fstSeekState = fstEnum.seekCeil(text);
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      if (term == null) {
        return SeekStatus.END;
      }
      return text.equals(term) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
    }

    @Override
    public void seekExact(BytesRef target, TermState state) throws IOException {
      if (!target.equals(term)) {
        assert state instanceof IntBlockTermState;
        termState.copyFrom(state);
        term = BytesRef.deepCopyOf(target);
        isTermStateCurrent = true;
        needReSeekInNext = true;
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public int docFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.postings(fieldInfo, termState, reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.impacts(fieldInfo, termState, flags);
    }

    @Override
    public TermState termState() throws IOException {
      updateTermStateIfNeeded();
      return termState.clone();
    }

    @Override
    public BytesRef next() throws IOException {
      if (needReSeekInNext) {
        fstSeekState = fstEnum.seekExact(term);
        assert fstSeekState != null;
      }
      fstSeekState = fstEnum.next();
      term = fstSeekState == null ? null : fstSeekState.input;
      isTermStateCurrent = false;
      needReSeekInNext = false;
      return term;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException("By ord lookup not supported.");
    }
  }

  final class RandomAccessIntersectTermsEnum extends TermsEnum {
    private AttributeSource attrs;

    private BytesRefBuilder term;

    private boolean isTermStateCurrent;

    private IntBlockTermState termState;

    private final PrimitiveLongFST fst;

    private final FST.BytesReader fstReader;

    private final ByteRunnable fsa;

    private PrimitiveLongFSTOutputs fstOutputs = PrimitiveLongFSTOutputs.getSingleton();

    private final TermDataReaderProvider.TermDataReader termDataReader;

    private Frame[] stack;

    private int level;

    private boolean pending;

    private final class Frame {
      /* fst stats */
      PrimitiveLongArc fstArc;
      long output;
      /* automaton stats */
      int fsaState;

      Frame() {
        this.fstArc = new PrimitiveLongArc();
        this.fsaState = -1;
      }

      @Override
      public String toString() {
        return "arc=" + fstArc + " state=" + fsaState;
      }
    }

    /**
     * Inspired by {@link org.apache.lucene.codecs.memory.FSTTermsReader}'s IntersectTermsEnum
     */
    RandomAccessIntersectTermsEnum(CompiledAutomaton compiled, BytesRef startTerm)
        throws IOException {
      termState = (IntBlockTermState) lucene99PostingsReader.newTermState();
      fst = termsDict.termsIndex().primitiveLongFST();
      fstReader = fst.getBytesReader();
      fsa = compiled.getByteRunnable();
      termDataReader = termsDict.termDataReaderProvider().newReader();

      stack = new Frame[16];
      for (int i = 0; i < stack.length; i++) {
        this.stack[i] = new Frame();
      }
      loadVirtualFrame(newFrame());
      level = 0;

      pushFrame(loadFirstFrame(newFrame()));
      if (startTerm == null) {
        pending = isAccept(topFrame());
      } else {
        doSeekCeil(startTerm);
        pending =
            (term == null || !startTerm.equals(term.get()))
                && isValid(topFrame())
                && isAccept(topFrame());
      }
    }

    @Override
    public BytesRef next() throws IOException {
      if (pending) {
        pending = false;
        updateTermStateIfNeeded();
        return term();
      }
      isTermStateCurrent = false;
      DFS:
      while (level > 0) {
        Frame frame = newFrame();
        if (loadExpandFrame(topFrame(), frame) != null) { // has valid target
          pushFrame(frame);
          if (isAccept(frame)) { // gotcha
            break;
          }
          continue; // check next target
        }
        frame = popFrame();
        while (level > 0) {
          if (loadNextFrame(topFrame(), frame) != null) { // has valid sibling
            pushFrame(frame);
            if (isAccept(frame)) { // gotcha
              break DFS;
            }
            continue DFS; // check next target
          }
          frame = popFrame();
        }
        return null;
      }
      if (term != null) {
        updateTermStateIfNeeded();
      }
      return term();
    }

    private long accumulateOutput() {
      long output = 0;
      int upto = 0;
      Frame last, next;
      last = stack[1];
      while (upto != level) {
        upto++;
        next = stack[upto];
        output = fstOutputs.add(next.output, output);
        last = next;
      }
      if (last.fstArc.isFinal()) {
        output = fstOutputs.add(output, last.fstArc.nextFinalOutput());
      }
      return output;
    }

    private BytesRef doSeekCeil(BytesRef target) throws IOException {
      Frame frame = null;
      int label, upto = 0, limit = target.length;
      while (upto < limit) { // to target prefix, or ceil label (rewind prefix)
        frame = newFrame();
        label = target.bytes[target.offset + upto] & 0xff;
        frame = loadCeilFrame(label, topFrame(), frame);
        if (frame == null || frame.fstArc.label() != label) {
          break;
        }
        assert isValid(frame); // target must be fetched from automaton
        pushFrame(frame);
        upto++;
      }
      if (upto == limit) { // got target
        return term();
      }
      if (frame != null) { // got larger term('s prefix)
        pushFrame(frame);
        return isAccept(frame) ? term() : next();
      }
      while (level > 0) { // got target's prefix, advance to larger term
        frame = popFrame();
        while (level > 0 && !canRewind(frame)) {
          frame = popFrame();
        }
        if (loadNextFrame(topFrame(), frame) != null) {
          pushFrame(frame);
          return isAccept(frame) ? term() : next();
        }
      }
      return null;
    }

    /** Load frame for target arc(node) on fst */
    Frame loadExpandFrame(Frame top, Frame frame) throws IOException {
      if (!canGrow(top)) {
        return null;
      }
      frame.fstArc = fst.readFirstRealTargetArc(top.fstArc.target(), frame.fstArc, fstReader);
      frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label());
      // if (TEST) System.out.println(" loadExpand frame="+frame);
      if (frame.fsaState == -1) {
        return loadNextFrame(top, frame);
      }
      frame.output = frame.fstArc.output();
      return frame;
    }

    Frame loadCeilFrame(int label, Frame top, Frame frame) throws IOException {
      PrimitiveLongArc arc = frame.fstArc;
      arc = Util.readCeilArc(label, fst, top.fstArc, arc, fstReader);
      if (arc == null) {
        return null;
      }
      frame.fsaState = fsa.step(top.fsaState, arc.label());
      if (frame.fsaState == -1) {
        return loadNextFrame(top, frame);
      }
      frame.output = frame.fstArc.output();
      return frame;
    }

    /** Load frame for sibling arc(node) on fst */
    Frame loadNextFrame(Frame top, Frame frame) throws IOException {
      if (!canRewind(frame)) {
        return null;
      }
      while (!frame.fstArc.isLast()) {
        frame.fstArc = fst.readNextRealArc(frame.fstArc, fstReader);
        frame.fsaState = fsa.step(top.fsaState, frame.fstArc.label());
        if (frame.fsaState != -1) {
          break;
        }
      }
      if (frame.fsaState == -1) {
        return null;
      }
      frame.output = frame.fstArc.output();
      return frame;
    }

    void updateTermStateIfNeeded() throws IOException {
      if (!isTermStateCurrent) {
        long fstOutput = accumulateOutput();
        TermsIndex.TypeAndOrd typeAndOrd = TermsIndex.decodeLong(fstOutput);
        termState =
            termDataReader.getTermState(
                typeAndOrd.termType(), typeAndOrd.ord(), fieldInfo.getIndexOptions());
        isTermStateCurrent = true;
      }
    }

    @Override
    public AttributeSource attributes() {
      if (attrs == null) {
        attrs = new AttributeSource();
      }
      return attrs;
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef term() throws IOException {
      return term == null ? null : term.get();
    }

    @Override
    public int docFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
      updateTermStateIfNeeded();
      return termState.totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.postings(fieldInfo, termState, reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      updateTermStateIfNeeded();
      return lucene99PostingsReader.impacts(fieldInfo, termState, flags);
    }

    @Override
    public TermState termState() throws IOException {
      updateTermStateIfNeeded();
      return termState.clone();
    }

    /** Virtual frame, never pop */
    Frame loadVirtualFrame(Frame frame) {
      frame.output = fstOutputs.getNoOutput();
      frame.fsaState = -1;
      return frame;
    }

    Frame newFrame() {
      if (level + 1 == stack.length) {
        final Frame[] temp =
            new Frame[ArrayUtil.oversize(level + 2, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(stack, 0, temp, 0, stack.length);
        for (int i = stack.length; i < temp.length; i++) {
          temp[i] = new Frame();
        }
        stack = temp;
      }
      return stack[level + 1];
    }

    Frame topFrame() {
      return stack[level];
    }

    boolean isAccept(Frame frame) { // reach a term both fst&fsa accepts
      return fsa.isAccept(frame.fsaState) && frame.fstArc.isFinal();
    }

    boolean isValid(Frame frame) { // reach a prefix both fst&fsa won't reject
      return /*frame != null &&*/ frame.fsaState != -1;
    }

    boolean canGrow(Frame frame) { // can walk forward on both fst&fsa
      return frame.fsaState != -1 && PrimitiveLongFST.targetHasArcs(frame.fstArc);
    }

    boolean canRewind(Frame frame) { // can jump to sibling
      return !frame.fstArc.isLast();
    }

    void pushFrame(Frame frame) {
      term = grow(frame.fstArc.label());
      level++;
    }

    Frame popFrame() {
      term = shrink();
      level--;
      return stack[level + 1];
    }

    Frame loadFirstFrame(Frame frame) {
      frame.fstArc = fst.getFirstArc(frame.fstArc);
      frame.output = frame.fstArc.output();
      frame.fsaState = 0;
      return frame;
    }

    BytesRefBuilder grow(int label) {
      if (term == null) {
        term = new BytesRefBuilder();
      } else {
        term.append((byte) label);
      }
      return term;
    }

    BytesRefBuilder shrink() {
      if (term.length() == 0) {
        term = null;
      } else {
        term.setLength(term.length() - 1);
      }
      return term;
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException("By ord lookup not supported.");
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(BytesRef target, TermState state) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
