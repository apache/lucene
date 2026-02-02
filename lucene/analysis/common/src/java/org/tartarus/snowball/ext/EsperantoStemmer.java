// Generated from esperanto.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from esperanto.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class EsperantoStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("", -1, 14),
    new Among("-", 0, 13),
    new Among("cx", 0, 1),
    new Among("gx", 0, 2),
    new Among("hx", 0, 3),
    new Among("jx", 0, 4),
    new Among("q", 0, 12),
    new Among("sx", 0, 5),
    new Among("ux", 0, 6),
    new Among("w", 0, 12),
    new Among("x", 0, 12),
    new Among("y", 0, 12),
    new Among("\u00E1", 0, 7),
    new Among("\u00E9", 0, 8),
    new Among("\u00ED", 0, 9),
    new Among("\u00F3", 0, 10),
    new Among("\u00FA", 0, 11)
  };

  private static final Among[] a_1 = {
    new Among("as", -1, -1),
    new Among("i", -1, -1),
    new Among("is", 1, -1),
    new Among("os", -1, -1),
    new Among("u", -1, -1),
    new Among("us", 4, -1)
  };

  private static final Among[] a_2 = {
    new Among("ci", -1, -1),
    new Among("gi", -1, -1),
    new Among("hi", -1, -1),
    new Among("li", -1, -1),
    new Among("ili", 3, -1),
    new Among("\u015Dli", 3, -1),
    new Among("mi", -1, -1),
    new Among("ni", -1, -1),
    new Among("oni", 7, -1),
    new Among("ri", -1, -1),
    new Among("si", -1, -1),
    new Among("vi", -1, -1),
    new Among("ivi", 11, -1),
    new Among("\u011Di", -1, -1),
    new Among("\u015Di", -1, -1),
    new Among("i\u015Di", 14, -1),
    new Among("mal\u015Di", 14, -1)
  };

  private static final Among[] a_3 = {
    new Among("amb", -1, -1),
    new Among("bald", -1, -1),
    new Among("malbald", 1, -1),
    new Among("morg", -1, -1),
    new Among("postmorg", 3, -1),
    new Among("adi", -1, -1),
    new Among("hodi", -1, -1),
    new Among("ank", -1, -1),
    new Among("\u0109irk", -1, -1),
    new Among("tut\u0109irk", 8, -1),
    new Among("presk", -1, -1),
    new Among("almen", -1, -1),
    new Among("apen", -1, -1),
    new Among("hier", -1, -1),
    new Among("anta\u016Dhier", 13, -1),
    new Among("malgr", -1, -1),
    new Among("ankor", -1, -1),
    new Among("kontr", -1, -1),
    new Among("anstat", -1, -1),
    new Among("kvaz", -1, -1)
  };

  private static final Among[] a_4 = {new Among("aliu", -1, -1), new Among("unu", -1, -1)};

  private static final Among[] a_5 = {
    new Among("aha", -1, -1),
    new Among("haha", 0, -1),
    new Among("haleluja", -1, -1),
    new Among("hola", -1, -1),
    new Among("hosana", -1, -1),
    new Among("maltra", -1, -1),
    new Among("hura", -1, -1),
    new Among("\u0125a\u0125a", -1, -1),
    new Among("ekde", -1, -1),
    new Among("elde", -1, -1),
    new Among("disde", -1, -1),
    new Among("ehe", -1, -1),
    new Among("maltre", -1, -1),
    new Among("dirlididi", -1, -1),
    new Among("malpli", -1, -1),
    new Among("mal\u0109i", -1, -1),
    new Among("malkaj", -1, -1),
    new Among("amen", -1, -1),
    new Among("tamen", 17, -1),
    new Among("oho", -1, -1),
    new Among("maltro", -1, -1),
    new Among("minus", -1, -1),
    new Among("uhu", -1, -1),
    new Among("muu", -1, -1)
  };

  private static final Among[] a_6 = {
    new Among("tri", -1, -1), new Among("du", -1, -1), new Among("unu", -1, -1)
  };

  private static final Among[] a_7 = {new Among("dek", -1, -1), new Among("cent", -1, -1)};

  private static final Among[] a_8 = {
    new Among("k", -1, -1),
    new Among("kelk", 0, -1),
    new Among("nen", -1, -1),
    new Among("t", -1, -1),
    new Among("mult", 3, -1),
    new Among("samt", 3, -1),
    new Among("\u0109", -1, -1)
  };

  private static final Among[] a_9 = {
    new Among("a", -1, -1),
    new Among("e", -1, -1),
    new Among("i", -1, -1),
    new Among("j", -1, 1),
    new Among("aj", 3, -1),
    new Among("oj", 3, -1),
    new Among("n", -1, 1),
    new Among("an", 6, -1),
    new Among("en", 6, -1),
    new Among("jn", 6, 1),
    new Among("ajn", 9, -1),
    new Among("ojn", 9, -1),
    new Among("on", 6, -1),
    new Among("o", -1, -1),
    new Among("as", -1, -1),
    new Among("is", -1, -1),
    new Among("os", -1, -1),
    new Among("us", -1, -1),
    new Among("u", -1, -1)
  };

  private static final char[] g_vowel = {17, 65, 16};

  private static final char[] g_aou = {1, 64, 16};

  private static final char[] g_digit = {255, 3};

  private boolean r_canonical_form() {
    boolean B_foreign;
    int among_var;
    B_foreign = false;
    while (true) {
      int v_1 = cursor;
      lab0:
      {
        bra = cursor;
        among_var = find_among(a_0);
        ket = cursor;
        switch (among_var) {
          case 1:
            slice_from("\u0109");
            break;
          case 2:
            slice_from("\u011D");
            break;
          case 3:
            slice_from("\u0125");
            break;
          case 4:
            slice_from("\u0135");
            break;
          case 5:
            slice_from("\u015D");
            break;
          case 6:
            slice_from("\u016D");
            break;
          case 7:
            slice_from("a");
            B_foreign = true;
            break;
          case 8:
            slice_from("e");
            B_foreign = true;
            break;
          case 9:
            slice_from("i");
            B_foreign = true;
            break;
          case 10:
            slice_from("o");
            B_foreign = true;
            break;
          case 11:
            slice_from("u");
            B_foreign = true;
            break;
          case 12:
            B_foreign = true;
            break;
          case 13:
            B_foreign = false;
            break;
          case 14:
            if (cursor >= limit) {
              break lab0;
            }
            cursor++;
            break;
        }
        continue;
      }
      cursor = v_1;
      break;
    }
    return !B_foreign;
  }

  private boolean r_initial_apostrophe() {
    bra = cursor;
    if (!(eq_s("'"))) {
      return false;
    }
    ket = cursor;
    if (!(eq_s("st"))) {
      return false;
    }
    if (find_among(a_1) == 0) {
      return false;
    }
    if (cursor < limit) {
      return false;
    }
    slice_from("e");
    return true;
  }

  private boolean r_pronoun() {
    ket = cursor;
    int v_1 = limit - cursor;
    lab0:
    {
      if (!(eq_s_b("n"))) {
        cursor = limit - v_1;
        break lab0;
      }
    }
    bra = cursor;
    if (find_among_b(a_2) == 0) {
      return false;
    }
    lab1:
    {
      int v_2 = limit - cursor;
      lab2:
      {
        if (cursor > limit_backward) {
          break lab2;
        }
        break lab1;
      }
      cursor = limit - v_2;
      if (!(eq_s_b("-"))) {
        return false;
      }
    }
    slice_del();
    return true;
  }

  private boolean r_final_apostrophe() {
    ket = cursor;
    if (!(eq_s_b("'"))) {
      return false;
    }
    bra = cursor;
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        if (!(eq_s_b("l"))) {
          break lab1;
        }
        if (cursor > limit_backward) {
          break lab1;
        }
        slice_from("a");
        break lab0;
      }
      cursor = limit - v_1;
      lab2:
      {
        if (!(eq_s_b("un"))) {
          break lab2;
        }
        if (cursor > limit_backward) {
          break lab2;
        }
        slice_from("u");
        break lab0;
      }
      cursor = limit - v_1;
      lab3:
      {
        if (find_among_b(a_3) == 0) {
          break lab3;
        }
        lab4:
        {
          int v_2 = limit - cursor;
          lab5:
          {
            if (cursor > limit_backward) {
              break lab5;
            }
            break lab4;
          }
          cursor = limit - v_2;
          if (!(eq_s_b("-"))) {
            break lab3;
          }
        }
        slice_from("a\u016D");
        break lab0;
      }
      cursor = limit - v_1;
      slice_from("o");
    }
    return true;
  }

  private boolean r_ujn_suffix() {
    ket = cursor;
    int v_1 = limit - cursor;
    lab0:
    {
      if (!(eq_s_b("n"))) {
        cursor = limit - v_1;
        break lab0;
      }
    }
    int v_2 = limit - cursor;
    lab1:
    {
      if (!(eq_s_b("j"))) {
        cursor = limit - v_2;
        break lab1;
      }
    }
    bra = cursor;
    if (find_among_b(a_4) == 0) {
      return false;
    }
    lab2:
    {
      int v_3 = limit - cursor;
      lab3:
      {
        if (cursor > limit_backward) {
          break lab3;
        }
        break lab2;
      }
      cursor = limit - v_3;
      if (!(eq_s_b("-"))) {
        return false;
      }
    }
    slice_del();
    return true;
  }

  private boolean r_uninflected() {
    if (find_among_b(a_5) == 0) {
      return false;
    }
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        if (cursor > limit_backward) {
          break lab1;
        }
        break lab0;
      }
      cursor = limit - v_1;
      if (!(eq_s_b("-"))) {
        return false;
      }
    }
    return true;
  }

  private boolean r_merged_numeral() {
    if (find_among_b(a_6) == 0) {
      return false;
    }
    return find_among_b(a_7) != 0;
  }

  private boolean r_correlative() {
    ket = cursor;
    bra = cursor;
    int v_1 = limit - cursor;
    lab0:
    {
      int v_2 = limit - cursor;
      lab1:
      {
        int v_3 = limit - cursor;
        lab2:
        {
          if (!(eq_s_b("n"))) {
            cursor = limit - v_3;
            break lab2;
          }
        }
        bra = cursor;
        if (!(eq_s_b("e"))) {
          break lab1;
        }
        break lab0;
      }
      cursor = limit - v_2;
      int v_4 = limit - cursor;
      lab3:
      {
        if (!(eq_s_b("n"))) {
          cursor = limit - v_4;
          break lab3;
        }
      }
      int v_5 = limit - cursor;
      lab4:
      {
        if (!(eq_s_b("j"))) {
          cursor = limit - v_5;
          break lab4;
        }
      }
      bra = cursor;
      if (!(in_grouping_b(g_aou, 97, 117))) {
        return false;
      }
    }
    if (!(eq_s_b("i"))) {
      return false;
    }
    int v_6 = limit - cursor;
    lab5:
    {
      if (find_among_b(a_8) == 0) {
        cursor = limit - v_6;
        break lab5;
      }
    }
    lab6:
    {
      int v_7 = limit - cursor;
      lab7:
      {
        if (cursor > limit_backward) {
          break lab7;
        }
        break lab6;
      }
      cursor = limit - v_7;
      if (!(eq_s_b("-"))) {
        return false;
      }
    }
    cursor = limit - v_1;
    slice_del();
    return true;
  }

  private boolean r_long_word() {
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        for (int v_2 = 2; v_2 > 0; v_2--) {
          if (!go_out_grouping_b(g_vowel, 97, 117)) {
            break lab1;
          }
          cursor--;
        }
        break lab0;
      }
      cursor = limit - v_1;
      lab2:
      {
        golab3:
        while (true) {
          lab4:
          {
            if (!(eq_s_b("-"))) {
              break lab4;
            }
            break golab3;
          }
          if (cursor <= limit_backward) {
            break lab2;
          }
          cursor--;
        }
        if (cursor <= limit_backward) {
          break lab2;
        }
        cursor--;
        break lab0;
      }
      cursor = limit - v_1;
      if (!go_out_grouping_b(g_digit, 48, 57)) {
        return false;
      }
      cursor--;
    }
    return true;
  }

  private boolean r_standard_suffix() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_9);
    if (among_var == 0) {
      return false;
    }
    switch (among_var) {
      case 1:
        int v_1 = limit - cursor;
        lab0:
        {
          int v_2 = limit - cursor;
          lab1:
          {
            if (!(eq_s_b("-"))) {
              break lab1;
            }
            break lab0;
          }
          cursor = limit - v_2;
          if (!(in_grouping_b(g_digit, 48, 57))) {
            return false;
          }
        }
        cursor = limit - v_1;
        break;
    }
    int v_3 = limit - cursor;
    lab2:
    {
      if (!(eq_s_b("-"))) {
        cursor = limit - v_3;
        break lab2;
      }
    }
    bra = cursor;
    slice_del();
    return true;
  }

  @Override
  public boolean stem() {
    int v_1 = cursor;
    if (!r_canonical_form()) {
      return false;
    }
    cursor = v_1;
    int v_2 = cursor;
    r_initial_apostrophe();
    cursor = v_2;
    limit_backward = cursor;
    cursor = limit;
    {
      int v_3 = limit - cursor;
      lab0:
      {
        if (!r_pronoun()) {
          break lab0;
        }
        return false;
      }
      cursor = limit - v_3;
    }
    int v_4 = limit - cursor;
    r_final_apostrophe();
    cursor = limit - v_4;
    {
      int v_5 = limit - cursor;
      lab1:
      {
        if (!r_correlative()) {
          break lab1;
        }
        return false;
      }
      cursor = limit - v_5;
    }
    {
      int v_6 = limit - cursor;
      lab2:
      {
        if (!r_uninflected()) {
          break lab2;
        }
        return false;
      }
      cursor = limit - v_6;
    }
    {
      int v_7 = limit - cursor;
      lab3:
      {
        if (!r_merged_numeral()) {
          break lab3;
        }
        return false;
      }
      cursor = limit - v_7;
    }
    {
      int v_8 = limit - cursor;
      lab4:
      {
        if (!r_ujn_suffix()) {
          break lab4;
        }
        return false;
      }
      cursor = limit - v_8;
    }
    int v_9 = limit - cursor;
    if (!r_long_word()) {
      return false;
    }
    cursor = limit - v_9;
    if (!r_standard_suffix()) {
      return false;
    }
    cursor = limit_backward;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof EsperantoStemmer;
  }

  @Override
  public int hashCode() {
    return EsperantoStemmer.class.getName().hashCode();
  }
}
