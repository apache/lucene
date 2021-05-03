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
package org.apache.lucene.analysis.icu;

import com.ibm.icu.text.UnicodeSet;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.List;

import org.apache.lucene.analysis.charfilter.BaseCharFilter;

import static com.ibm.icu.text.UnicodeSet.SpanCondition.SIMPLE;
import static com.ibm.icu.text.UnicodeSet.SpanCondition.NOT_CONTAINED;

final class ICUBypassCharFilter {

  private static final boolean INITIAL_MATCH_STATE = true;
  static final int END_OF_SPAN = -2;

  /**
   * To be extended by CharFilters that should be able to "pretend" to reach EOF,
   * have internal state cleared via {@link #clearState(int)}, and be reused with further
   * upstream input.
   */
  interface FilterAware {
    void clearState(int offsetBypass);
  }

  static final class Pre extends BaseCharFilter {

    private final UnicodeSet filter;
    private boolean matchState = INITIAL_MATCH_STATE;
    private final char[] buf = new char[4096];
    private final CharBuffer cbuf = CharBuffer.wrap(buf);
    private int outLimit;
    private int bufStart;
    private int bufEnd;

    public Pre(Reader in, UnicodeSet filter) {
      super(in);
      this.filter = filter;
    }

    /**
     * Extends normal {@link Reader#read(char[], int, int)} contract with special return value
     * of {@value END_OF_SPAN}, which indicates the end of a span of input (wrt the configured
     * {@link #filter}, and which should trigger flush in downstream consumers.
     */
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      final int read = input.read(buf, bufEnd, buf.length - bufEnd);
      if (read != -1) {
        bufEnd += read;
      } else if (outLimit >= bufEnd) {
        // no content remains in the buffer, so return EOF
        return -1;
      }
      this.cbuf.position(bufStart);
      this.cbuf.limit(bufEnd);
      final int spanLen = filter.span(this.cbuf, matchState ? SIMPLE : NOT_CONTAINED);
      if (outLimit >= bufStart && spanLen == 0) {
        // nothing old in the buffer and we didn't find anything new for current matchState
        if (matchState) {
          matchState = false;
          return -1;
        } else {
          matchState = true;
          return END_OF_SPAN;
        }
      }
      bufStart += spanLen;
      final int ret = Math.min(bufStart - outLimit, len);
      System.arraycopy(buf, outLimit, cbuf, off, ret);
      outLimit += ret;
      if (outLimit >= bufEnd) {
        // we've output everything; simply reset
        outLimit = 0;
        bufStart = 0;
        bufEnd = 0;
      } else if (outLimit<<1 > this.buf.length) {
        // shift for more space
        bufEnd -= outLimit; // bufEnd now same as new length of internal buffer
        bufStart -= outLimit; // also shift this by the amount we can discard
        System.arraycopy(buf, outLimit, buf, 0, bufEnd);
        outLimit = 0;
      }
      return ret;
    }
  }

  static final class Post extends BaseCharFilter {

    private boolean matchState = INITIAL_MATCH_STATE;
    private final Pre upstream;
    private final List<FilterAware> conditional;

    public Post(Reader in, Pre upstream, List<FilterAware> conditional) {
      super(in);
      this.upstream = upstream;
      this.conditional = conditional;
    }

    private void resetConditional(int offsetBypass) {
      for (FilterAware f : conditional) {
        f.clearState(offsetBypass);
      }
    }

    private int offsetBypass = 0;

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      for (;;) {
        final Reader activeInput;
        if (matchState) {
          activeInput = this.input;
          if (offsetBypass > 0) {
            // entering matchState==true after accumulating bypass offsets; reset
            resetConditional(offsetBypass);
            offsetBypass = 0;
          }
        } else {
          activeInput = upstream;
        }
        final int ret = activeInput.read(cbuf, off, len);
        switch (ret) {
          case END_OF_SPAN:
            // special case, we know it's not actually the end of input
            matchState = !matchState; // toggle
            break;
          case -1:
            if (matchState) {
              // receiving input from a source that may or may not be END_OF_SPAN-aware
              matchState = false; // toggle
              break;
            } else {
              // receiving input from upstream Pre; this is _actually_ EOF
              return -1;
            }
          default:
            if (!matchState) {
              offsetBypass += ret;
            }
            return ret;
        }
      }
    }
  }
}
