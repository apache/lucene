// Generated from tamil.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from tamil.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class TamilStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("\u0BB5\u0BC1", -1, 3),
    new Among("\u0BB5\u0BC2", -1, 4),
    new Among("\u0BB5\u0BCA", -1, 2),
    new Among("\u0BB5\u0BCB", -1, 1)
  };

  private static final Among[] a_1 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B99", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9E", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BA8", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BAE", -1, -1),
    new Among("\u0BAF", -1, -1),
    new Among("\u0BB5", -1, -1)
  };

  private static final Among[] a_2 = {
    new Among("\u0BBF", -1, -1), new Among("\u0BC0", -1, -1), new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_3 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_4 = {
    new Among("", -1, 2), new Among("\u0BC8", 0, 1), new Among("\u0BCD", 0, 1)
  };

  private static final Among[] a_5 = {
    new Among("\u0BA8\u0BCD\u0BA4", -1, 1),
    new Among("\u0BAF", -1, 1),
    new Among("\u0BB5", -1, 1),
    new Among("\u0BA9\u0BC1", -1, 8),
    new Among("\u0BC1\u0B95\u0BCD", -1, 7),
    new Among("\u0BC1\u0B95\u0BCD\u0B95\u0BCD", -1, 7),
    new Among("\u0B9F\u0BCD\u0B95\u0BCD", -1, 3),
    new Among("\u0BB1\u0BCD\u0B95\u0BCD", -1, 4),
    new Among("\u0B99\u0BCD", -1, 9),
    new Among("\u0B9F\u0BCD\u0B9F\u0BCD", -1, 5),
    new Among("\u0BA4\u0BCD\u0BA4\u0BCD", -1, 6),
    new Among("\u0BA8\u0BCD\u0BA4\u0BCD", -1, 1),
    new Among("\u0BA8\u0BCD", -1, 1),
    new Among("\u0B9F\u0BCD\u0BAA\u0BCD", -1, 3),
    new Among("\u0BAF\u0BCD", -1, 2),
    new Among("\u0BA9\u0BCD\u0BB1\u0BCD", -1, 4),
    new Among("\u0BB5\u0BCD", -1, 1)
  };

  private static final Among[] a_6 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9F", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BB1", -1, -1)
  };

  private static final Among[] a_7 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9F", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BB1", -1, -1)
  };

  private static final Among[] a_8 = {
    new Among("\u0B9E", -1, -1),
    new Among("\u0BA3", -1, -1),
    new Among("\u0BA8", -1, -1),
    new Among("\u0BA9", -1, -1),
    new Among("\u0BAE", -1, -1),
    new Among("\u0BAF", -1, -1),
    new Among("\u0BB0", -1, -1),
    new Among("\u0BB2", -1, -1),
    new Among("\u0BB3", -1, -1),
    new Among("\u0BB4", -1, -1),
    new Among("\u0BB5", -1, -1)
  };

  private static final Among[] a_9 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1),
    new Among("\u0BCD", -1, -1)
  };

  private static final Among[] a_10 = {
    new Among("\u0B85", -1, -1), new Among("\u0B87", -1, -1), new Among("\u0B89", -1, -1)
  };

  private static final Among[] a_11 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B99", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9E", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BA8", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BAE", -1, -1),
    new Among("\u0BAF", -1, -1),
    new Among("\u0BB5", -1, -1)
  };

  private static final Among[] a_12 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9F", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BB1", -1, -1)
  };

  private static final Among[] a_13 = {
    new Among("\u0B95\u0BB3\u0BCD", -1, 4),
    new Among("\u0BC1\u0B99\u0BCD\u0B95\u0BB3\u0BCD", 0, 1),
    new Among("\u0B9F\u0BCD\u0B95\u0BB3\u0BCD", 0, 3),
    new Among("\u0BB1\u0BCD\u0B95\u0BB3\u0BCD", 0, 2)
  };

  private static final Among[] a_14 = {
    new Among("\u0BBE", -1, -1), new Among("\u0BC7", -1, -1), new Among("\u0BCB", -1, -1)
  };

  private static final Among[] a_15 = {
    new Among("\u0BAA\u0BBF", -1, -1), new Among("\u0BB5\u0BBF", -1, -1)
  };

  private static final Among[] a_16 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_17 = {
    new Among("\u0BAA\u0B9F\u0BCD\u0B9F", -1, 3),
    new Among("\u0BAA\u0B9F\u0BCD\u0B9F\u0BA3", -1, 3),
    new Among("\u0BA4\u0BBE\u0BA9", -1, 3),
    new Among("\u0BAA\u0B9F\u0BBF\u0BA4\u0BBE\u0BA9", 2, 3),
    new Among("\u0BC6\u0BA9", -1, 1),
    new Among("\u0BBE\u0B95\u0BBF\u0BAF", -1, 1),
    new Among("\u0B95\u0BC1\u0BB0\u0BBF\u0BAF", -1, 3),
    new Among("\u0BC1\u0B9F\u0BC8\u0BAF", -1, 1),
    new Among("\u0BB2\u0BCD\u0BB2", -1, 2),
    new Among("\u0BC1\u0BB3\u0BCD\u0BB3", -1, 1),
    new Among("\u0BBE\u0B95\u0BBF", -1, 1),
    new Among("\u0BAA\u0B9F\u0BBF", -1, 3),
    new Among("\u0BBF\u0BA9\u0BCD\u0BB1\u0BBF", -1, 1),
    new Among("\u0BAA\u0BB1\u0BCD\u0BB1\u0BBF", -1, 3),
    new Among("\u0BAA\u0B9F\u0BC1", -1, 3),
    new Among("\u0BB5\u0BBF\u0B9F\u0BC1", -1, 3),
    new Among("\u0BAA\u0B9F\u0BCD\u0B9F\u0BC1", -1, 3),
    new Among("\u0BB5\u0BBF\u0B9F\u0BCD\u0B9F\u0BC1", -1, 3),
    new Among("\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1", -1, 3),
    new Among("\u0BC6\u0BA9\u0BCD\u0BB1\u0BC1", -1, 1),
    new Among("\u0BC1\u0B9F\u0BC8", -1, 1),
    new Among("\u0BBF\u0BB2\u0BCD\u0BB2\u0BC8", -1, 1),
    new Among("\u0BC1\u0B9F\u0BA9\u0BCD", -1, 1),
    new Among("\u0BBF\u0B9F\u0BAE\u0BCD", -1, 1),
    new Among("\u0BC6\u0BB2\u0BCD\u0BB2\u0BBE\u0BAE\u0BCD", -1, 3),
    new Among("\u0BC6\u0BA9\u0BC1\u0BAE\u0BCD", -1, 1)
  };

  private static final Among[] a_18 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_19 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_20 = {
    new Among("\u0BB5\u0BBF\u0B9F", -1, 2),
    new Among("\u0BC0", -1, 7),
    new Among("\u0BCA\u0B9F\u0BC1", -1, 2),
    new Among("\u0BCB\u0B9F\u0BC1", -1, 2),
    new Among("\u0BA4\u0BC1", -1, 6),
    new Among("\u0BBF\u0BB0\u0BC1\u0BA8\u0BCD\u0BA4\u0BC1", 4, 2),
    new Among("\u0BBF\u0BA9\u0BCD\u0BB1\u0BC1", -1, 2),
    new Among("\u0BC1\u0B9F\u0BC8", -1, 2),
    new Among("\u0BA9\u0BC8", -1, 1),
    new Among("\u0B95\u0BA3\u0BCD", -1, 1),
    new Among("\u0BBF\u0BA9\u0BCD", -1, 3),
    new Among("\u0BAE\u0BC1\u0BA9\u0BCD", -1, 1),
    new Among("\u0BBF\u0B9F\u0BAE\u0BCD", -1, 4),
    new Among("\u0BBF\u0BB1\u0BCD", -1, 2),
    new Among("\u0BAE\u0BC7\u0BB1\u0BCD", -1, 1),
    new Among("\u0BB2\u0BCD", -1, 5),
    new Among("\u0BBE\u0BAE\u0BB2\u0BCD", 15, 2),
    new Among("\u0BBE\u0BB2\u0BCD", 15, 2),
    new Among("\u0BBF\u0BB2\u0BCD", 15, 2),
    new Among("\u0BAE\u0BC7\u0BB2\u0BCD", 15, 1),
    new Among("\u0BC1\u0BB3\u0BCD", -1, 2),
    new Among("\u0B95\u0BC0\u0BB4\u0BCD", -1, 1)
  };

  private static final Among[] a_21 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9F", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BB1", -1, -1)
  };

  private static final Among[] a_22 = {
    new Among("\u0B95", -1, -1),
    new Among("\u0B9A", -1, -1),
    new Among("\u0B9F", -1, -1),
    new Among("\u0BA4", -1, -1),
    new Among("\u0BAA", -1, -1),
    new Among("\u0BB1", -1, -1)
  };

  private static final Among[] a_23 = {
    new Among("\u0B85", -1, -1),
    new Among("\u0B86", -1, -1),
    new Among("\u0B87", -1, -1),
    new Among("\u0B88", -1, -1),
    new Among("\u0B89", -1, -1),
    new Among("\u0B8A", -1, -1),
    new Among("\u0B8E", -1, -1),
    new Among("\u0B8F", -1, -1),
    new Among("\u0B90", -1, -1),
    new Among("\u0B92", -1, -1),
    new Among("\u0B93", -1, -1),
    new Among("\u0B94", -1, -1)
  };

  private static final Among[] a_24 = {
    new Among("\u0BBE", -1, -1),
    new Among("\u0BBF", -1, -1),
    new Among("\u0BC0", -1, -1),
    new Among("\u0BC1", -1, -1),
    new Among("\u0BC2", -1, -1),
    new Among("\u0BC6", -1, -1),
    new Among("\u0BC7", -1, -1),
    new Among("\u0BC8", -1, -1)
  };

  private static final Among[] a_25 = {
    new Among("\u0B95", -1, 1),
    new Among("\u0BA4", -1, 1),
    new Among("\u0BA9", -1, 1),
    new Among("\u0BAA", -1, 1),
    new Among("\u0BAF", -1, 1),
    new Among("\u0BBE", -1, 5),
    new Among("\u0B95\u0BC1", -1, 6),
    new Among("\u0BAA\u0B9F\u0BC1", -1, 1),
    new Among("\u0BA4\u0BC1", -1, 3),
    new Among("\u0BBF\u0BB1\u0BCD\u0BB1\u0BC1", -1, 1),
    new Among("\u0BA9\u0BC8", -1, 1),
    new Among("\u0BB5\u0BC8", -1, 1),
    new Among("\u0BA9\u0BA9\u0BCD", -1, 1),
    new Among("\u0BAA\u0BA9\u0BCD", -1, 1),
    new Among("\u0BB5\u0BA9\u0BCD", -1, 2),
    new Among("\u0BBE\u0BA9\u0BCD", -1, 4),
    new Among("\u0BA9\u0BBE\u0BA9\u0BCD", 15, 1),
    new Among("\u0BAE\u0BBF\u0BA9\u0BCD", -1, 1),
    new Among("\u0BA9\u0BC6\u0BA9\u0BCD", -1, 1),
    new Among("\u0BC7\u0BA9\u0BCD", -1, 5),
    new Among("\u0BA9\u0BAE\u0BCD", -1, 1),
    new Among("\u0BAA\u0BAE\u0BCD", -1, 1),
    new Among("\u0BBE\u0BAE\u0BCD", -1, 5),
    new Among("\u0B95\u0BC1\u0BAE\u0BCD", -1, 1),
    new Among("\u0B9F\u0BC1\u0BAE\u0BCD", -1, 5),
    new Among("\u0BA4\u0BC1\u0BAE\u0BCD", -1, 1),
    new Among("\u0BB1\u0BC1\u0BAE\u0BCD", -1, 1),
    new Among("\u0BC6\u0BAE\u0BCD", -1, 5),
    new Among("\u0BC7\u0BAE\u0BCD", -1, 5),
    new Among("\u0BCB\u0BAE\u0BCD", -1, 5),
    new Among("\u0BBE\u0BAF\u0BCD", -1, 5),
    new Among("\u0BA9\u0BB0\u0BCD", -1, 1),
    new Among("\u0BAA\u0BB0\u0BCD", -1, 1),
    new Among("\u0BC0\u0BAF\u0BB0\u0BCD", -1, 5),
    new Among("\u0BB5\u0BB0\u0BCD", -1, 1),
    new Among("\u0BBE\u0BB0\u0BCD", -1, 5),
    new Among("\u0BA9\u0BBE\u0BB0\u0BCD", 35, 1),
    new Among("\u0BAE\u0BBE\u0BB0\u0BCD", 35, 1),
    new Among("\u0B95\u0BCA\u0BA3\u0BCD\u0B9F\u0BBF\u0BB0\u0BCD", -1, 1),
    new Among("\u0BA9\u0BBF\u0BB0\u0BCD", -1, 5),
    new Among("\u0BC0\u0BB0\u0BCD", -1, 5),
    new Among("\u0BA9\u0BB3\u0BCD", -1, 1),
    new Among("\u0BAA\u0BB3\u0BCD", -1, 1),
    new Among("\u0BB5\u0BB3\u0BCD", -1, 1),
    new Among("\u0BBE\u0BB3\u0BCD", -1, 5),
    new Among("\u0BA9\u0BBE\u0BB3\u0BCD", 44, 1)
  };

  private static final Among[] a_26 = {
    new Among("\u0B95\u0BBF\u0BB1", -1, -1),
    new Among("\u0B95\u0BBF\u0BA9\u0BCD\u0BB1", -1, -1),
    new Among("\u0BBE\u0BA8\u0BBF\u0BA9\u0BCD\u0BB1", -1, -1),
    new Among("\u0B95\u0BBF\u0BB1\u0BCD", -1, -1),
    new Among("\u0B95\u0BBF\u0BA9\u0BCD\u0BB1\u0BCD", -1, -1),
    new Among("\u0BBE\u0BA8\u0BBF\u0BA9\u0BCD\u0BB1\u0BCD", -1, -1)
  };

  private boolean B_found_vetrumai_urupu;

  private boolean r_has_min_length() {
    return length > 4;
  }

  private boolean r_fix_va_start() {
    int among_var;
    bra = cursor;
    among_var = find_among(a_0);
    if (among_var == 0) {
      return false;
    }
    ket = cursor;
    switch (among_var) {
      case 1:
        slice_from("\u0B93");
        break;
      case 2:
        slice_from("\u0B92");
        break;
      case 3:
        slice_from("\u0B89");
        break;
      case 4:
        slice_from("\u0B8A");
        break;
    }
    return true;
  }

  private boolean r_fix_endings() {
    int v_1 = cursor;
    lab0:
    {
      while (true) {
        int v_2 = cursor;
        lab1:
        {
          if (!r_fix_ending()) {
            break lab1;
          }
          continue;
        }
        cursor = v_2;
        break;
      }
    }
    cursor = v_1;
    return true;
  }

  private boolean r_remove_question_prefixes() {
    bra = cursor;
    if (!(eq_s("\u0B8E"))) {
      return false;
    }
    if (find_among(a_1) == 0) {
      return false;
    }
    if (!(eq_s("\u0BCD"))) {
      return false;
    }
    ket = cursor;
    slice_del();
    int v_1 = cursor;
    r_fix_va_start();
    cursor = v_1;
    return true;
  }

  private boolean r_fix_ending() {
    int among_var;
    if (length <= 3) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        ket = cursor;
        among_var = find_among_b(a_5);
        if (among_var == 0) {
          break lab1;
        }
        bra = cursor;
        switch (among_var) {
          case 1:
            slice_del();
            break;
          case 2:
            int v_2 = limit - cursor;
            if (find_among_b(a_2) == 0) {
              break lab1;
            }
            cursor = limit - v_2;
            slice_del();
            break;
          case 3:
            slice_from("\u0BB3\u0BCD");
            break;
          case 4:
            slice_from("\u0BB2\u0BCD");
            break;
          case 5:
            slice_from("\u0B9F\u0BC1");
            break;
          case 6:
            if (!B_found_vetrumai_urupu) {
              break lab1;
            }
            {
              int v_3 = limit - cursor;
              lab2:
              {
                if (!(eq_s_b("\u0BC8"))) {
                  break lab2;
                }
                break lab1;
              }
              cursor = limit - v_3;
            }
            slice_from("\u0BAE\u0BCD");
            break;
          case 7:
            slice_from("\u0BCD");
            break;
          case 8:
            {
              int v_4 = limit - cursor;
              lab3:
              {
                if (find_among_b(a_3) == 0) {
                  break lab3;
                }
                break lab1;
              }
              cursor = limit - v_4;
            }
            slice_del();
            break;
          case 9:
            among_var = find_among_b(a_4);
            switch (among_var) {
              case 1:
                slice_del();
                break;
              case 2:
                slice_from("\u0BAE\u0BCD");
                break;
            }
            break;
        }
        break lab0;
      }
      cursor = limit - v_1;
      ket = cursor;
      if (!(eq_s_b("\u0BCD"))) {
        return false;
      }
      lab4:
      {
        int v_5 = limit - cursor;
        lab5:
        {
          if (find_among_b(a_6) == 0) {
            break lab5;
          }
          int v_6 = limit - cursor;
          lab6:
          {
            if (!(eq_s_b("\u0BCD"))) {
              cursor = limit - v_6;
              break lab6;
            }
            if (find_among_b(a_7) == 0) {
              cursor = limit - v_6;
              break lab6;
            }
          }
          bra = cursor;
          slice_del();
          break lab4;
        }
        cursor = limit - v_5;
        lab7:
        {
          if (find_among_b(a_8) == 0) {
            break lab7;
          }
          bra = cursor;
          if (!(eq_s_b("\u0BCD"))) {
            break lab7;
          }
          slice_del();
          break lab4;
        }
        cursor = limit - v_5;
        int v_7 = limit - cursor;
        if (find_among_b(a_9) == 0) {
          return false;
        }
        cursor = limit - v_7;
        bra = cursor;
        slice_del();
      }
    }
    cursor = limit_backward;
    return true;
  }

  private boolean r_remove_pronoun_prefixes() {
    bra = cursor;
    if (find_among(a_10) == 0) {
      return false;
    }
    if (find_among(a_11) == 0) {
      return false;
    }
    if (!(eq_s("\u0BCD"))) {
      return false;
    }
    ket = cursor;
    slice_del();
    int v_1 = cursor;
    r_fix_va_start();
    cursor = v_1;
    return true;
  }

  private boolean r_remove_plural_suffix() {
    int among_var;
    limit_backward = cursor;
    cursor = limit;
    ket = cursor;
    among_var = find_among_b(a_13);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        lab0:
        {
          int v_1 = limit - cursor;
          lab1:
          {
            if (find_among_b(a_12) == 0) {
              break lab1;
            }
            slice_from("\u0BC1\u0B99\u0BCD");
            break lab0;
          }
          cursor = limit - v_1;
          slice_from("\u0BCD");
        }
        break;
      case 2:
        slice_from("\u0BB2\u0BCD");
        break;
      case 3:
        slice_from("\u0BB3\u0BCD");
        break;
      case 4:
        slice_del();
        break;
    }
    cursor = limit_backward;
    return true;
  }

  private boolean r_remove_question_suffixes() {
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    int v_1 = limit - cursor;
    lab0:
    {
      ket = cursor;
      if (find_among_b(a_14) == 0) {
        break lab0;
      }
      bra = cursor;
      slice_from("\u0BCD");
    }
    cursor = limit - v_1;
    cursor = limit_backward;
    r_fix_endings();
    return true;
  }

  private boolean r_remove_command_suffixes() {
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    ket = cursor;
    if (find_among_b(a_15) == 0) {
      return false;
    }
    bra = cursor;
    slice_del();
    cursor = limit_backward;
    return true;
  }

  private boolean r_remove_um() {
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    ket = cursor;
    if (!(eq_s_b("\u0BC1\u0BAE\u0BCD"))) {
      return false;
    }
    bra = cursor;
    slice_from("\u0BCD");
    cursor = limit_backward;
    int v_1 = cursor;
    r_fix_ending();
    cursor = v_1;
    return true;
  }

  private boolean r_remove_common_word_endings() {
    int among_var;
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    ket = cursor;
    among_var = find_among_b(a_17);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        slice_from("\u0BCD");
        break;
      case 2:
        {
          int v_1 = limit - cursor;
          lab0:
          {
            if (find_among_b(a_16) == 0) {
              break lab0;
            }
            return false;
          }
          cursor = limit - v_1;
        }
        slice_from("\u0BCD");
        break;
      case 3:
        slice_del();
        break;
    }
    cursor = limit_backward;
    r_fix_endings();
    return true;
  }

  private boolean r_remove_vetrumai_urupukal() {
    int among_var;
    B_found_vetrumai_urupu = false;
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        int v_2 = limit - cursor;
        ket = cursor;
        among_var = find_among_b(a_20);
        if (among_var == 0) {
          break lab1;
        }
        bra = cursor;
        switch (among_var) {
          case 1:
            slice_del();
            break;
          case 2:
            slice_from("\u0BCD");
            break;
          case 3:
            {
              int v_3 = limit - cursor;
              lab2:
              {
                if (!(eq_s_b("\u0BAE"))) {
                  break lab2;
                }
                break lab1;
              }
              cursor = limit - v_3;
            }
            slice_from("\u0BCD");
            break;
          case 4:
            if (length < 7) {
              break lab1;
            }
            slice_from("\u0BCD");
            break;
          case 5:
            {
              int v_4 = limit - cursor;
              lab3:
              {
                if (find_among_b(a_18) == 0) {
                  break lab3;
                }
                break lab1;
              }
              cursor = limit - v_4;
            }
            slice_from("\u0BCD");
            break;
          case 6:
            {
              int v_5 = limit - cursor;
              lab4:
              {
                if (find_among_b(a_19) == 0) {
                  break lab4;
                }
                break lab1;
              }
              cursor = limit - v_5;
            }
            slice_del();
            break;
          case 7:
            slice_from("\u0BBF");
            break;
        }
        cursor = limit - v_2;
        break lab0;
      }
      cursor = limit - v_1;
      int v_6 = limit - cursor;
      ket = cursor;
      if (!(eq_s_b("\u0BC8"))) {
        return false;
      }
      lab5:
      {
        int v_7 = limit - cursor;
        lab6:
        {
          {
            int v_8 = limit - cursor;
            lab7:
            {
              if (find_among_b(a_21) == 0) {
                break lab7;
              }
              break lab6;
            }
            cursor = limit - v_8;
          }
          break lab5;
        }
        cursor = limit - v_7;
        int v_9 = limit - cursor;
        if (find_among_b(a_22) == 0) {
          return false;
        }
        if (!(eq_s_b("\u0BCD"))) {
          return false;
        }
        cursor = limit - v_9;
      }
      bra = cursor;
      slice_from("\u0BCD");
      cursor = limit - v_6;
    }
    B_found_vetrumai_urupu = true;
    int v_10 = limit - cursor;
    lab8:
    {
      ket = cursor;
      if (!(eq_s_b("\u0BBF\u0BA9\u0BCD"))) {
        break lab8;
      }
      bra = cursor;
      slice_from("\u0BCD");
    }
    cursor = limit - v_10;
    cursor = limit_backward;
    r_fix_endings();
    return true;
  }

  private boolean r_remove_tense_suffixes() {
    while (true) {
      int v_1 = cursor;
      lab0:
      {
        if (!r_remove_tense_suffix()) {
          break lab0;
        }
        continue;
      }
      cursor = v_1;
      break;
    }
    return true;
  }

  private boolean r_remove_tense_suffix() {
    boolean B_found_a_match;
    int among_var;
    B_found_a_match = false;
    if (!r_has_min_length()) {
      return false;
    }
    limit_backward = cursor;
    cursor = limit;
    int v_1 = limit - cursor;
    lab0:
    {
      int v_2 = limit - cursor;
      ket = cursor;
      among_var = find_among_b(a_25);
      if (among_var == 0) {
        break lab0;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          slice_del();
          break;
        case 2:
          {
            int v_3 = limit - cursor;
            lab1:
            {
              if (find_among_b(a_23) == 0) {
                break lab1;
              }
              break lab0;
            }
            cursor = limit - v_3;
          }
          slice_del();
          break;
        case 3:
          {
            int v_4 = limit - cursor;
            lab2:
            {
              if (find_among_b(a_24) == 0) {
                break lab2;
              }
              break lab0;
            }
            cursor = limit - v_4;
          }
          slice_del();
          break;
        case 4:
          {
            int v_5 = limit - cursor;
            lab3:
            {
              if (!(eq_s_b("\u0B9A"))) {
                break lab3;
              }
              break lab0;
            }
            cursor = limit - v_5;
          }
          slice_from("\u0BCD");
          break;
        case 5:
          slice_from("\u0BCD");
          break;
        case 6:
          int v_6 = limit - cursor;
          if (!(eq_s_b("\u0BCD"))) {
            break lab0;
          }
          cursor = limit - v_6;
          slice_del();
          break;
      }
      B_found_a_match = true;
      cursor = limit - v_2;
    }
    cursor = limit - v_1;
    int v_7 = limit - cursor;
    lab4:
    {
      ket = cursor;
      if (find_among_b(a_26) == 0) {
        break lab4;
      }
      bra = cursor;
      slice_del();
      B_found_a_match = true;
    }
    cursor = limit - v_7;
    cursor = limit_backward;
    r_fix_endings();
    return B_found_a_match;
  }

  @Override
  public boolean stem() {
    B_found_vetrumai_urupu = false;
    int v_1 = cursor;
    r_fix_ending();
    cursor = v_1;
    if (!r_has_min_length()) {
      return false;
    }
    int v_2 = cursor;
    r_remove_question_prefixes();
    cursor = v_2;
    int v_3 = cursor;
    r_remove_pronoun_prefixes();
    cursor = v_3;
    int v_4 = cursor;
    r_remove_question_suffixes();
    cursor = v_4;
    int v_5 = cursor;
    r_remove_um();
    cursor = v_5;
    int v_6 = cursor;
    r_remove_common_word_endings();
    cursor = v_6;
    int v_7 = cursor;
    r_remove_vetrumai_urupukal();
    cursor = v_7;
    int v_8 = cursor;
    r_remove_plural_suffix();
    cursor = v_8;
    int v_9 = cursor;
    r_remove_command_suffixes();
    cursor = v_9;
    int v_10 = cursor;
    r_remove_tense_suffixes();
    cursor = v_10;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TamilStemmer;
  }

  @Override
  public int hashCode() {
    return TamilStemmer.class.getName().hashCode();
  }
}
