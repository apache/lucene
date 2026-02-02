// Generated from indonesian.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from indonesian.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class IndonesianStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("kah", -1, 1), new Among("lah", -1, 1), new Among("pun", -1, 1)
  };

  private static final Among[] a_1 = {
    new Among("nya", -1, 1), new Among("ku", -1, 1), new Among("mu", -1, 1)
  };

  private static final Among[] a_2 = {new Among("i", -1, 2), new Among("an", -1, 1)};

  private static final Among[] a_3 = {
    new Among("di", -1, 1),
    new Among("ke", -1, 3),
    new Among("me", -1, 1),
    new Among("mem", 2, 5),
    new Among("men", 2, 2),
    new Among("meng", 4, 1),
    new Among("pem", -1, 6),
    new Among("pen", -1, 4),
    new Among("peng", 7, 3),
    new Among("ter", -1, 1)
  };

  private static final Among[] a_4 = {new Among("be", -1, 2), new Among("pe", -1, 1)};

  private static final char[] g_vowel = {17, 65, 16};

  private int I_prefix;
  private int I_measure;

  private boolean r_remove_particle() {
    ket = cursor;
    if (find_among_b(a_0) == 0) {
      return false;
    }
    bra = cursor;
    slice_del();
    I_measure -= 1;
    return true;
  }

  private boolean r_remove_possessive_pronoun() {
    ket = cursor;
    if (find_among_b(a_1) == 0) {
      return false;
    }
    bra = cursor;
    slice_del();
    I_measure -= 1;
    return true;
  }

  private boolean r_remove_suffix() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_2);
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
            if (I_prefix == 3) {
              break lab1;
            }
            if (I_prefix == 2) {
              break lab1;
            }
            if (!(eq_s_b("k"))) {
              break lab1;
            }
            bra = cursor;
            break lab0;
          }
          cursor = limit - v_1;
          if (I_prefix == 1) {
            return false;
          }
        }
        break;
      case 2:
        if (I_prefix > 2) {
          return false;
        }
        {
          int v_2 = limit - cursor;
          lab2:
          {
            if (!(eq_s_b("s"))) {
              break lab2;
            }
            return false;
          }
          cursor = limit - v_2;
        }
        break;
    }
    slice_del();
    I_measure -= 1;
    return true;
  }

  private boolean r_remove_first_order_prefix() {
    int among_var;
    bra = cursor;
    among_var = find_among(a_3);
    if (among_var == 0) {
      return false;
    }
    ket = cursor;
    switch (among_var) {
      case 1:
        slice_del();
        I_prefix = 1;
        I_measure -= 1;
        break;
      case 2:
        lab0:
        {
          int v_1 = cursor;
          lab1:
          {
            if (!(eq_s("y"))) {
              break lab1;
            }
            int v_2 = cursor;
            if (!(in_grouping(g_vowel, 97, 117))) {
              break lab1;
            }
            cursor = v_2;
            ket = cursor;
            slice_from("s");
            I_prefix = 1;
            I_measure -= 1;
            break lab0;
          }
          cursor = v_1;
          slice_del();
          I_prefix = 1;
          I_measure -= 1;
        }
        break;
      case 3:
        slice_del();
        I_prefix = 3;
        I_measure -= 1;
        break;
      case 4:
        lab2:
        {
          int v_3 = cursor;
          lab3:
          {
            if (!(eq_s("y"))) {
              break lab3;
            }
            int v_4 = cursor;
            if (!(in_grouping(g_vowel, 97, 117))) {
              break lab3;
            }
            cursor = v_4;
            ket = cursor;
            slice_from("s");
            I_prefix = 3;
            I_measure -= 1;
            break lab2;
          }
          cursor = v_3;
          slice_del();
          I_prefix = 3;
          I_measure -= 1;
        }
        break;
      case 5:
        I_prefix = 1;
        I_measure -= 1;
        lab4:
        {
          int v_5 = cursor;
          lab5:
          {
            int v_6 = cursor;
            if (!(in_grouping(g_vowel, 97, 117))) {
              break lab5;
            }
            cursor = v_6;
            slice_from("p");
            break lab4;
          }
          cursor = v_5;
          slice_del();
        }
        break;
      case 6:
        I_prefix = 3;
        I_measure -= 1;
        lab6:
        {
          int v_7 = cursor;
          lab7:
          {
            int v_8 = cursor;
            if (!(in_grouping(g_vowel, 97, 117))) {
              break lab7;
            }
            cursor = v_8;
            slice_from("p");
            break lab6;
          }
          cursor = v_7;
          slice_del();
        }
        break;
    }
    return true;
  }

  private boolean r_remove_second_order_prefix() {
    int among_var;
    bra = cursor;
    among_var = find_among(a_4);
    if (among_var == 0) {
      return false;
    }
    switch (among_var) {
      case 1:
        lab0:
        {
          int v_1 = cursor;
          lab1:
          {
            if (!(eq_s("r"))) {
              break lab1;
            }
            ket = cursor;
            I_prefix = 2;
            break lab0;
          }
          cursor = v_1;
          lab2:
          {
            if (!(eq_s("l"))) {
              break lab2;
            }
            ket = cursor;
            if (!(eq_s("ajar"))) {
              break lab2;
            }
            break lab0;
          }
          cursor = v_1;
          ket = cursor;
          I_prefix = 2;
        }
        break;
      case 2:
        lab3:
        {
          int v_2 = cursor;
          lab4:
          {
            if (!(eq_s("r"))) {
              break lab4;
            }
            ket = cursor;
            break lab3;
          }
          cursor = v_2;
          lab5:
          {
            if (!(eq_s("l"))) {
              break lab5;
            }
            ket = cursor;
            if (!(eq_s("ajar"))) {
              break lab5;
            }
            break lab3;
          }
          cursor = v_2;
          ket = cursor;
          if (!(out_grouping(g_vowel, 97, 117))) {
            return false;
          }
          if (!(eq_s("er"))) {
            return false;
          }
        }
        I_prefix = 4;
        break;
    }
    I_measure -= 1;
    slice_del();
    return true;
  }

  @Override
  public boolean stem() {
    I_measure = 0;
    int v_1 = cursor;
    lab0:
    {
      while (true) {
        int v_2 = cursor;
        lab1:
        {
          if (!go_out_grouping(g_vowel, 97, 117)) {
            break lab1;
          }
          cursor++;
          I_measure += 1;
          continue;
        }
        cursor = v_2;
        break;
      }
    }
    cursor = v_1;
    if (I_measure <= 2) {
      return false;
    }
    I_prefix = 0;
    limit_backward = cursor;
    cursor = limit;
    int v_3 = limit - cursor;
    r_remove_particle();
    cursor = limit - v_3;
    if (I_measure <= 2) {
      return false;
    }
    int v_4 = limit - cursor;
    r_remove_possessive_pronoun();
    cursor = limit - v_4;
    cursor = limit_backward;
    if (I_measure <= 2) {
      return false;
    }
    lab2:
    {
      int v_5 = cursor;
      lab3:
      {
        int v_6 = cursor;
        if (!r_remove_first_order_prefix()) {
          break lab3;
        }
        int v_7 = cursor;
        lab4:
        {
          int v_8 = cursor;
          if (I_measure <= 2) {
            break lab4;
          }
          limit_backward = cursor;
          cursor = limit;
          if (!r_remove_suffix()) {
            break lab4;
          }
          cursor = limit_backward;
          cursor = v_8;
          if (I_measure <= 2) {
            break lab4;
          }
          if (!r_remove_second_order_prefix()) {
            break lab4;
          }
        }
        cursor = v_7;
        cursor = v_6;
        break lab2;
      }
      cursor = v_5;
      int v_9 = cursor;
      r_remove_second_order_prefix();
      cursor = v_9;
      int v_10 = cursor;
      lab5:
      {
        if (I_measure <= 2) {
          break lab5;
        }
        limit_backward = cursor;
        cursor = limit;
        if (!r_remove_suffix()) {
          break lab5;
        }
        cursor = limit_backward;
      }
      cursor = v_10;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof IndonesianStemmer;
  }

  @Override
  public int hashCode() {
    return IndonesianStemmer.class.getName().hashCode();
  }
}
