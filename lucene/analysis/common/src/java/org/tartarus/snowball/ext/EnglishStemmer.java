// Generated from english.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from english.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class EnglishStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("arsen", -1, -1),
    new Among("commun", -1, -1),
    new Among("emerg", -1, -1),
    new Among("gener", -1, -1),
    new Among("inter", -1, -1),
    new Among("later", -1, -1),
    new Among("organ", -1, -1),
    new Among("past", -1, -1),
    new Among("univers", -1, -1)
  };

  private static final Among[] a_1 = {
    new Among("'", -1, 1), new Among("'s'", 0, 1), new Among("'s", -1, 1)
  };

  private static final Among[] a_2 = {
    new Among("ied", -1, 2),
    new Among("s", -1, 3),
    new Among("ies", 1, 2),
    new Among("sses", 1, 1),
    new Among("ss", 1, -1),
    new Among("us", 1, -1)
  };

  private static final Among[] a_3 = {
    new Among("succ", -1, 1), new Among("proc", -1, 1), new Among("exc", -1, 1)
  };

  private static final Among[] a_4 = {
    new Among("even", -1, 2),
    new Among("cann", -1, 2),
    new Among("inn", -1, 2),
    new Among("earr", -1, 2),
    new Among("herr", -1, 2),
    new Among("out", -1, 2),
    new Among("y", -1, 1)
  };

  private static final Among[] a_5 = {
    new Among("", -1, -1),
    new Among("ed", 0, 2),
    new Among("eed", 1, 1),
    new Among("ing", 0, 3),
    new Among("edly", 0, 2),
    new Among("eedly", 4, 1),
    new Among("ingly", 0, 2)
  };

  private static final Among[] a_6 = {
    new Among("", -1, 3),
    new Among("bb", 0, 2),
    new Among("dd", 0, 2),
    new Among("ff", 0, 2),
    new Among("gg", 0, 2),
    new Among("bl", 0, 1),
    new Among("mm", 0, 2),
    new Among("nn", 0, 2),
    new Among("pp", 0, 2),
    new Among("rr", 0, 2),
    new Among("at", 0, 1),
    new Among("tt", 0, 2),
    new Among("iz", 0, 1)
  };

  private static final Among[] a_7 = {
    new Among("anci", -1, 3),
    new Among("enci", -1, 2),
    new Among("ogi", -1, 14),
    new Among("li", -1, 16),
    new Among("bli", 3, 12),
    new Among("abli", 4, 4),
    new Among("alli", 3, 8),
    new Among("fulli", 3, 9),
    new Among("lessli", 3, 15),
    new Among("ousli", 3, 10),
    new Among("entli", 3, 5),
    new Among("aliti", -1, 8),
    new Among("biliti", -1, 12),
    new Among("iviti", -1, 11),
    new Among("tional", -1, 1),
    new Among("ational", 14, 7),
    new Among("alism", -1, 8),
    new Among("ation", -1, 7),
    new Among("ization", 17, 6),
    new Among("izer", -1, 6),
    new Among("ator", -1, 7),
    new Among("iveness", -1, 11),
    new Among("fulness", -1, 9),
    new Among("ousness", -1, 10),
    new Among("ogist", -1, 13)
  };

  private static final Among[] a_8 = {
    new Among("icate", -1, 4),
    new Among("ative", -1, 6),
    new Among("alize", -1, 3),
    new Among("iciti", -1, 4),
    new Among("ical", -1, 4),
    new Among("tional", -1, 1),
    new Among("ational", 5, 2),
    new Among("ful", -1, 5),
    new Among("ness", -1, 5)
  };

  private static final Among[] a_9 = {
    new Among("ic", -1, 1),
    new Among("ance", -1, 1),
    new Among("ence", -1, 1),
    new Among("able", -1, 1),
    new Among("ible", -1, 1),
    new Among("ate", -1, 1),
    new Among("ive", -1, 1),
    new Among("ize", -1, 1),
    new Among("iti", -1, 1),
    new Among("al", -1, 1),
    new Among("ism", -1, 1),
    new Among("ion", -1, 2),
    new Among("er", -1, 1),
    new Among("ous", -1, 1),
    new Among("ant", -1, 1),
    new Among("ent", -1, 1),
    new Among("ment", 15, 1),
    new Among("ement", 16, 1)
  };

  private static final Among[] a_10 = {new Among("e", -1, 1), new Among("l", -1, 2)};

  private static final Among[] a_11 = {
    new Among("andes", -1, -1),
    new Among("atlas", -1, -1),
    new Among("bias", -1, -1),
    new Among("cosmos", -1, -1),
    new Among("early", -1, 6),
    new Among("gently", -1, 4),
    new Among("howe", -1, -1),
    new Among("idly", -1, 3),
    new Among("news", -1, -1),
    new Among("only", -1, 7),
    new Among("singly", -1, 8),
    new Among("skies", -1, 2),
    new Among("skis", -1, 1),
    new Among("sky", -1, -1),
    new Among("ugly", -1, 5)
  };

  private static final char[] g_aeo = {17, 64};

  private static final char[] g_v = {17, 65, 16, 1};

  private static final char[] g_v_WXY = {1, 17, 65, 208, 1};

  private static final char[] g_valid_LI = {55, 141, 2};

  private boolean B_Y_found;
  private int I_p2;
  private int I_p1;

  private boolean r_prelude() {
    B_Y_found = false;
    int v_1 = cursor;
    lab0:
    {
      bra = cursor;
      if (!(eq_s("'"))) {
        break lab0;
      }
      ket = cursor;
      slice_del();
    }
    cursor = v_1;
    int v_2 = cursor;
    lab1:
    {
      bra = cursor;
      if (!(eq_s("y"))) {
        break lab1;
      }
      ket = cursor;
      slice_from("Y");
      B_Y_found = true;
    }
    cursor = v_2;
    int v_3 = cursor;
    lab2:
    {
      while (true) {
        int v_4 = cursor;
        lab3:
        {
          golab4:
          while (true) {
            int v_5 = cursor;
            lab5:
            {
              if (!(in_grouping(g_v, 97, 121))) {
                break lab5;
              }
              bra = cursor;
              if (!(eq_s("y"))) {
                break lab5;
              }
              ket = cursor;
              cursor = v_5;
              break golab4;
            }
            cursor = v_5;
            if (cursor >= limit) {
              break lab3;
            }
            cursor++;
          }
          slice_from("Y");
          B_Y_found = true;
          continue;
        }
        cursor = v_4;
        break;
      }
    }
    cursor = v_3;
    return true;
  }

  private boolean r_mark_regions() {
    I_p1 = limit;
    I_p2 = limit;
    int v_1 = cursor;
    lab0:
    {
      lab1:
      {
        int v_2 = cursor;
        lab2:
        {
          if (find_among(a_0) == 0) {
            break lab2;
          }
          break lab1;
        }
        cursor = v_2;
        if (!go_out_grouping(g_v, 97, 121)) {
          break lab0;
        }
        cursor++;
        if (!go_in_grouping(g_v, 97, 121)) {
          break lab0;
        }
        cursor++;
      }
      I_p1 = cursor;
      if (!go_out_grouping(g_v, 97, 121)) {
        break lab0;
      }
      cursor++;
      if (!go_in_grouping(g_v, 97, 121)) {
        break lab0;
      }
      cursor++;
      I_p2 = cursor;
    }
    cursor = v_1;
    return true;
  }

  private boolean r_shortv() {
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        if (!(out_grouping_b(g_v_WXY, 89, 121))) {
          break lab1;
        }
        if (!(in_grouping_b(g_v, 97, 121))) {
          break lab1;
        }
        if (!(out_grouping_b(g_v, 97, 121))) {
          break lab1;
        }
        break lab0;
      }
      cursor = limit - v_1;
      lab2:
      {
        if (!(out_grouping_b(g_v, 97, 121))) {
          break lab2;
        }
        if (!(in_grouping_b(g_v, 97, 121))) {
          break lab2;
        }
        if (cursor > limit_backward) {
          break lab2;
        }
        break lab0;
      }
      cursor = limit - v_1;
      if (!(eq_s_b("past"))) {
        return false;
      }
    }
    return true;
  }

  private boolean r_R1() {
    return I_p1 <= cursor;
  }

  private boolean r_R2() {
    return I_p2 <= cursor;
  }

  private boolean r_Step_1a() {
    int among_var;
    int v_1 = limit - cursor;
    lab0:
    {
      ket = cursor;
      if (find_among_b(a_1) == 0) {
        cursor = limit - v_1;
        break lab0;
      }
      bra = cursor;
      slice_del();
    }
    ket = cursor;
    among_var = find_among_b(a_2);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        slice_from("ss");
        break;
      case 2:
        lab1:
        {
          int v_2 = limit - cursor;
          lab2:
          {
            {
              int c = cursor - 2;
              if (c < limit_backward) {
                break lab2;
              }
              cursor = c;
            }
            slice_from("i");
            break lab1;
          }
          cursor = limit - v_2;
          slice_from("ie");
        }
        break;
      case 3:
        if (cursor <= limit_backward) {
          return false;
        }
        cursor--;
        if (!go_out_grouping_b(g_v, 97, 121)) {
          return false;
        }
        cursor--;
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_Step_1b() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_5);
    bra = cursor;
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        switch (among_var) {
          case 1:
            int v_2 = limit - cursor;
            lab2:
            {
              if (!r_R1()) {
                break lab2;
              }
              lab3:
              {
                int v_3 = limit - cursor;
                lab4:
                {
                  if (find_among_b(a_3) == 0) {
                    break lab4;
                  }
                  if (cursor > limit_backward) {
                    break lab4;
                  }
                  break lab3;
                }
                cursor = limit - v_3;
                slice_from("ee");
              }
            }
            cursor = limit - v_2;
            break;
          case 2:
            break lab1;
          case 3:
            among_var = find_among_b(a_4);
            if (among_var == 0) {
              break lab1;
            }
            switch (among_var) {
              case 1:
                int v_4 = limit - cursor;
                if (!(out_grouping_b(g_v, 97, 121))) {
                  break lab1;
                }
                if (cursor > limit_backward) {
                  break lab1;
                }
                cursor = limit - v_4;
                bra = cursor;
                slice_from("ie");
                break;
              case 2:
                if (cursor > limit_backward) {
                  break lab1;
                }
                break;
            }
            break;
        }
        break lab0;
      }
      cursor = limit - v_1;
      int v_5 = limit - cursor;
      if (!go_out_grouping_b(g_v, 97, 121)) {
        return false;
      }
      cursor--;
      cursor = limit - v_5;
      slice_del();
      ket = cursor;
      bra = cursor;
      int v_6 = limit - cursor;
      among_var = find_among_b(a_6);
      switch (among_var) {
        case 1:
          slice_from("e");
          return false;
        case 2:
          {
            int v_7 = limit - cursor;
            lab5:
            {
              if (!(in_grouping_b(g_aeo, 97, 111))) {
                break lab5;
              }
              if (cursor > limit_backward) {
                break lab5;
              }
              return false;
            }
            cursor = limit - v_7;
          }
          break;
        case 3:
          if (cursor != I_p1) {
            return false;
          }
          int v_8 = limit - cursor;
          if (!r_shortv()) {
            return false;
          }
          cursor = limit - v_8;
          slice_from("e");
          return false;
      }
      cursor = limit - v_6;
      ket = cursor;
      if (cursor <= limit_backward) {
        return false;
      }
      cursor--;
      bra = cursor;
      slice_del();
    }
    return true;
  }

  private boolean r_Step_1c() {
    ket = cursor;
    lab0:
    {
      int v_1 = limit - cursor;
      lab1:
      {
        if (!(eq_s_b("y"))) {
          break lab1;
        }
        break lab0;
      }
      cursor = limit - v_1;
      if (!(eq_s_b("Y"))) {
        return false;
      }
    }
    bra = cursor;
    if (!(out_grouping_b(g_v, 97, 121))) {
      return false;
    }
    lab2:
    {
      if (cursor > limit_backward) {
        break lab2;
      }
      return false;
    }
    slice_from("i");
    return true;
  }

  private boolean r_Step_2() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_7);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    if (!r_R1()) {
      return false;
    }
    switch (among_var) {
      case 1:
        slice_from("tion");
        break;
      case 2:
        slice_from("ence");
        break;
      case 3:
        slice_from("ance");
        break;
      case 4:
        slice_from("able");
        break;
      case 5:
        slice_from("ent");
        break;
      case 6:
        slice_from("ize");
        break;
      case 7:
        slice_from("ate");
        break;
      case 8:
        slice_from("al");
        break;
      case 9:
        slice_from("ful");
        break;
      case 10:
        slice_from("ous");
        break;
      case 11:
        slice_from("ive");
        break;
      case 12:
        slice_from("ble");
        break;
      case 13:
        slice_from("og");
        break;
      case 14:
        if (!(eq_s_b("l"))) {
          return false;
        }
        slice_from("og");
        break;
      case 15:
        slice_from("less");
        break;
      case 16:
        if (!(in_grouping_b(g_valid_LI, 99, 116))) {
          return false;
        }
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_Step_3() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_8);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    if (!r_R1()) {
      return false;
    }
    switch (among_var) {
      case 1:
        slice_from("tion");
        break;
      case 2:
        slice_from("ate");
        break;
      case 3:
        slice_from("al");
        break;
      case 4:
        slice_from("ic");
        break;
      case 5:
        slice_del();
        break;
      case 6:
        if (!r_R2()) {
          return false;
        }
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_Step_4() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_9);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    if (!r_R2()) {
      return false;
    }
    switch (among_var) {
      case 1:
        slice_del();
        break;
      case 2:
        lab0:
        {
          int v_1 = limit - cursor;
          lab1:
          {
            if (!(eq_s_b("s"))) {
              break lab1;
            }
            break lab0;
          }
          cursor = limit - v_1;
          if (!(eq_s_b("t"))) {
            return false;
          }
        }
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_Step_5() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_10);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        lab0:
        {
          lab1:
          {
            if (!r_R2()) {
              break lab1;
            }
            break lab0;
          }
          if (!r_R1()) {
            return false;
          }
          {
            int v_1 = limit - cursor;
            lab2:
            {
              if (!r_shortv()) {
                break lab2;
              }
              return false;
            }
            cursor = limit - v_1;
          }
        }
        slice_del();
        break;
      case 2:
        if (!r_R2()) {
          return false;
        }
        if (!(eq_s_b("l"))) {
          return false;
        }
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_exception1() {
    int among_var;
    bra = cursor;
    among_var = find_among(a_11);
    if (among_var == 0) {
      return false;
    }
    ket = cursor;
    if (cursor < limit) {
      return false;
    }
    switch (among_var) {
      case 1:
        slice_from("ski");
        break;
      case 2:
        slice_from("sky");
        break;
      case 3:
        slice_from("idl");
        break;
      case 4:
        slice_from("gentl");
        break;
      case 5:
        slice_from("ugli");
        break;
      case 6:
        slice_from("earli");
        break;
      case 7:
        slice_from("onli");
        break;
      case 8:
        slice_from("singl");
        break;
    }
    return true;
  }

  private boolean r_postlude() {
    if (!B_Y_found) {
      return false;
    }
    while (true) {
      int v_1 = cursor;
      lab0:
      {
        golab1:
        while (true) {
          int v_2 = cursor;
          lab2:
          {
            bra = cursor;
            if (!(eq_s("Y"))) {
              break lab2;
            }
            ket = cursor;
            cursor = v_2;
            break golab1;
          }
          cursor = v_2;
          if (cursor >= limit) {
            break lab0;
          }
          cursor++;
        }
        slice_from("y");
        continue;
      }
      cursor = v_1;
      break;
    }
    return true;
  }

  @Override
  public boolean stem() {
    lab0:
    {
      int v_1 = cursor;
      lab1:
      {
        if (!r_exception1()) {
          break lab1;
        }
        break lab0;
      }
      cursor = v_1;
      lab2:
      {
        {
          int v_2 = cursor;
          lab3:
          {
            {
              int c = cursor + 3;
              if (c > limit) {
                break lab3;
              }
              cursor = c;
            }
            break lab2;
          }
          cursor = v_2;
        }
        break lab0;
      }
      cursor = v_1;
      r_prelude();
      r_mark_regions();
      limit_backward = cursor;
      cursor = limit;
      int v_3 = limit - cursor;
      r_Step_1a();
      cursor = limit - v_3;
      int v_4 = limit - cursor;
      r_Step_1b();
      cursor = limit - v_4;
      int v_5 = limit - cursor;
      r_Step_1c();
      cursor = limit - v_5;
      int v_6 = limit - cursor;
      r_Step_2();
      cursor = limit - v_6;
      int v_7 = limit - cursor;
      r_Step_3();
      cursor = limit - v_7;
      int v_8 = limit - cursor;
      r_Step_4();
      cursor = limit - v_8;
      int v_9 = limit - cursor;
      r_Step_5();
      cursor = limit - v_9;
      cursor = limit_backward;
      int v_10 = cursor;
      r_postlude();
      cursor = v_10;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof EnglishStemmer;
  }

  @Override
  public int hashCode() {
    return EnglishStemmer.class.getName().hashCode();
  }
}
