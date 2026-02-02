/*
Copyright (c) 2001, Dr Martin Porter
Copyright (c) 2004,2005, Richard Boulton
Copyright (c) 2013, Yoshiki Shibukawa
Copyright (c) 2006-2025, Olly Betts
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

  1. Redistributions of source code must retain the above copyright notice,
     this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright notice,
     this list of conditions and the following disclaimer in the documentation
     and/or other materials provided with the distribution.
  3. Neither the name of the Snowball project nor the names of its contributors
     may be used to endorse or promote products derived from this software
     without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.tartarus.snowball;

import java.io.Serializable;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/** Base class for a snowball stemmer */
public class SnowballProgram implements Serializable {
  protected SnowballProgram() {
    cursor = 0;
    length = limit = 0;
    limit_backward = 0;
    bra = cursor;
    ket = limit;
  }

  static final long serialVersionUID = 2016072500L;

  /** Set the current string. */
  public void setCurrent(String value) {
    setCurrent(value.toCharArray(), value.length());
  }

  /** Get the current string. */
  public String getCurrent() {
    return new String(current, 0, length);
  }

  /**
   * Set the current string.
   *
   * @param text character array containing input
   * @param length valid length of text.
   */
  public void setCurrent(char[] text, int length) {
    current = text;
    cursor = 0;
    this.length = limit = length;
    limit_backward = 0;
    bra = cursor;
    ket = limit;
  }

  /**
   * Get the current buffer containing the stem.
   *
   * <p>NOTE: this may be a reference to a different character array than the one originally
   * provided with setCurrent, in the exceptional case that stemming produced a longer intermediate
   * or result string.
   *
   * <p>It is necessary to use {@link #getCurrentBufferLength()} to determine the valid length of
   * the returned buffer. For example, many words are stemmed simply by subtracting from the length
   * to remove suffixes.
   *
   * @see #getCurrentBufferLength()
   */
  public char[] getCurrentBuffer() {
    return current;
  }

  /**
   * Get the valid length of the character array in {@link #getCurrentBuffer()}.
   *
   * @return valid length of the array.
   */
  public int getCurrentBufferLength() {
    return length;
  }

  // current string
  protected char[] current;

  protected int cursor;
  protected int length;
  protected int limit;
  protected int limit_backward;
  protected int bra;
  protected int ket;

  public SnowballProgram(SnowballProgram other) {
    current = other.current;
    cursor = other.cursor;
    length = other.length;
    limit = other.limit;
    limit_backward = other.limit_backward;
    bra = other.bra;
    ket = other.ket;
  }

  protected void copy_from(SnowballProgram other) {
    current = other.current;
    cursor = other.cursor;
    length = other.length;
    limit = other.limit;
    limit_backward = other.limit_backward;
    bra = other.bra;
    ket = other.ket;
  }

  protected boolean in_grouping(char[] s, int min, int max) {
    if (cursor >= limit) return false;
    int ch = current[cursor];
    if (ch > max || ch < min) return false;
    ch -= min;
    if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return false;
    cursor++;
    return true;
  }

  protected boolean go_in_grouping(char[] s, int min, int max) {
    while (cursor < limit) {
      int ch = current[cursor];
      if (ch > max || ch < min) return true;
      ch -= min;
      if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return true;
      cursor++;
    }
    return false;
  }

  protected boolean in_grouping_b(char[] s, int min, int max) {
    if (cursor <= limit_backward) return false;
    int ch = current[cursor - 1];
    if (ch > max || ch < min) return false;
    ch -= min;
    if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return false;
    cursor--;
    return true;
  }

  protected boolean go_in_grouping_b(char[] s, int min, int max) {
    while (cursor > limit_backward) {
      int ch = current[cursor - 1];
      if (ch > max || ch < min) return true;
      ch -= min;
      if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return true;
      cursor--;
    }
    return false;
  }

  protected boolean out_grouping(char[] s, int min, int max) {
    if (cursor >= limit) return false;
    int ch = current[cursor];
    if (ch > max || ch < min) {
      cursor++;
      return true;
    }
    ch -= min;
    if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) {
      cursor++;
      return true;
    }
    return false;
  }

  protected boolean go_out_grouping(char[] s, int min, int max) {
    while (cursor < limit) {
      int ch = current[cursor];
      if (ch <= max && ch >= min) {
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) != 0) {
          return true;
        }
      }
      cursor++;
    }
    return false;
  }

  protected boolean out_grouping_b(char[] s, int min, int max) {
    if (cursor <= limit_backward) return false;
    int ch = current[cursor - 1];
    if (ch > max || ch < min) {
      cursor--;
      return true;
    }
    ch -= min;
    if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) {
      cursor--;
      return true;
    }
    return false;
  }

  protected boolean go_out_grouping_b(char[] s, int min, int max) {
    while (cursor > limit_backward) {
      int ch = current[cursor - 1];
      if (ch <= max && ch >= min) {
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) != 0) {
          return true;
        }
      }
      cursor--;
    }
    return false;
  }

  protected boolean eq_s(CharSequence s) {
    if (limit - cursor < s.length()) return false;
    int i;
    for (i = 0; i != s.length(); i++) {
      if (current[cursor + i] != s.charAt(i)) return false;
    }
    cursor += s.length();
    return true;
  }

  protected boolean eq_s_b(CharSequence s) {
    if (cursor - limit_backward < s.length()) return false;
    int i;
    for (i = 0; i != s.length(); i++) {
      if (current[cursor - s.length() + i] != s.charAt(i)) return false;
    }
    cursor -= s.length();
    return true;
  }

  protected int find_among(Among[] v) {
    int i = 0;
    int j = v.length;

    int c = cursor;
    int l = limit;

    int common_i = 0;
    int common_j = 0;

    boolean first_key_inspected = false;

    while (true) {
      int k = i + ((j - i) >> 1);
      int diff = 0;
      int common = common_i < common_j ? common_i : common_j; // smaller
      Among w = v[k];
      int i2;
      for (i2 = common; i2 < w.s.length; i2++) {
        if (c + common == l) {
          diff = -1;
          break;
        }
        diff = current[c + common] - w.s[i2];
        if (diff != 0) break;
        common++;
      }
      if (diff < 0) {
        j = k;
        common_j = common;
      } else {
        i = k;
        common_i = common;
      }
      if (j - i <= 1) {
        if (i > 0) break; // v->s has been inspected
        if (j == i) break; // only one item in v

        // - but now we need to go round once more to get
        // v->s inspected. This looks messy, but is actually
        // the optimal approach.

        if (first_key_inspected) break;
        first_key_inspected = true;
      }
    }
    while (true) {
      Among w = v[i];
      if (common_i >= w.s.length) {
        cursor = c + w.s.length;
        if (w.method == null) return w.result;
        try {
          if ((boolean) w.method.invokeExact(this)) {
            cursor = c + w.s.length;
            return w.result;
          }
        } catch (Error | RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new UndeclaredThrowableException(e);
        }
      }
      i = w.substring_i;
      if (i < 0) return 0;
    }
  }

  // find_among_b is for backwards processing. Same comments apply
  protected int find_among_b(Among[] v) {
    int i = 0;
    int j = v.length;

    int c = cursor;
    int lb = limit_backward;

    int common_i = 0;
    int common_j = 0;

    boolean first_key_inspected = false;

    while (true) {
      int k = i + ((j - i) >> 1);
      int diff = 0;
      int common = common_i < common_j ? common_i : common_j;
      Among w = v[k];
      int i2;
      for (i2 = w.s.length - 1 - common; i2 >= 0; i2--) {
        if (c - common == lb) {
          diff = -1;
          break;
        }
        diff = current[c - 1 - common] - w.s[i2];
        if (diff != 0) break;
        common++;
      }
      if (diff < 0) {
        j = k;
        common_j = common;
      } else {
        i = k;
        common_i = common;
      }
      if (j - i <= 1) {
        if (i > 0) break;
        if (j == i) break;
        if (first_key_inspected) break;
        first_key_inspected = true;
      }
    }
    while (true) {
      Among w = v[i];
      if (common_i >= w.s.length) {
        cursor = c - w.s.length;
        if (w.method == null) return w.result;
        try {
          if ((boolean) w.method.invokeExact(this)) {
            cursor = c - w.s.length;
            return w.result;
          }
        } catch (Error | RuntimeException e) {
          throw e;
        } catch (Throwable e) {
          throw new UndeclaredThrowableException(e);
        }
      }
      i = w.substring_i;
      if (i < 0) return 0;
    }
  }

  /* to replace chars between c_bra and c_ket in current by the
   * chars in s.
   */
  protected int replace_s(int c_bra, int c_ket, CharSequence s) {
    final int adjustment = s.length() - (c_ket - c_bra);
    final int newLength = length + adjustment;
    // resize if necessary
    if (newLength > current.length) {
      current = Arrays.copyOf(current, newLength);
    }
    // if the substring being replaced is longer or shorter than the
    // replacement, need to shift things around
    if (adjustment != 0 && c_ket < length) {
      System.arraycopy(current, c_ket, current, c_bra + s.length(), length - c_ket);
    }
    // insert the replacement text
    // Note, faster is s.getChars(0, s.length(), current, c_bra);
    // but would have to duplicate this method for both String and StringBuilder
    for (int i = 0; i < s.length(); i++) current[c_bra + i] = s.charAt(i);

    length += adjustment;
    limit += adjustment;
    if (cursor >= c_ket) cursor += adjustment;
    else if (cursor > c_bra) cursor = c_bra;
    return adjustment;
  }

  protected void slice_check() {
    assert bra >= 0 : "bra=" + bra;
    assert bra <= ket : "bra=" + bra + ",ket=" + ket;
    assert limit <= length : "limit=" + limit + ",length=" + length;
    assert ket <= limit : "ket=" + ket + ",limit=" + limit;
  }

  protected void slice_from(CharSequence s) {
    slice_check();
    replace_s(bra, ket, s);
    ket = bra + s.length();
  }

  protected void slice_del() {
    slice_from("");
  }

  protected void insert(int c_bra, int c_ket, CharSequence s) {
    int adjustment = replace_s(c_bra, c_ket, s);
    if (c_bra <= bra) bra += adjustment;
    if (c_bra <= ket) ket += adjustment;
  }

  /*
  extern void debug(struct SN_env * z, int number, int line_count)
  {   int i;
      int limit = SIZE(z->p);
      //if (number >= 0) printf("%3d (line %4d): '", number, line_count);
      if (number >= 0) printf("%3d (line %4d): [%d]'", number, line_count,limit);
      for (i = 0; i <= limit; i++)
      {   if (z->lb == i) printf("{");
          if (z->bra == i) printf("[");
          if (z->c == i) printf("|");
          if (z->ket == i) printf("]");
          if (z->l == i) printf("}");
          if (i < limit)
          {   int ch = z->p[i];
              if (ch == 0) ch = '#';
              printf("%c", ch);
          }
      }
      printf("'\n");
  }
  */

}
