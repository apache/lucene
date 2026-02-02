// Generated from dutch_porter.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from dutch_porter.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class Dutch_porterStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("", -1, 6),
    new Among("\u00E1", 0, 1),
    new Among("\u00E4", 0, 1),
    new Among("\u00E9", 0, 2),
    new Among("\u00EB", 0, 2),
    new Among("\u00ED", 0, 3),
    new Among("\u00EF", 0, 3),
    new Among("\u00F3", 0, 4),
    new Among("\u00F6", 0, 4),
    new Among("\u00FA", 0, 5),
    new Among("\u00FC", 0, 5)
  };

  private static final Among[] a_1 = {
    new Among("", -1, 3), new Among("I", 0, 2), new Among("Y", 0, 1)
  };

  private static final Among[] a_2 = {
    new Among("dd", -1, -1), new Among("kk", -1, -1), new Among("tt", -1, -1)
  };

  private static final Among[] a_3 = {
    new Among("ene", -1, 2),
    new Among("se", -1, 3),
    new Among("en", -1, 2),
    new Among("heden", 2, 1),
    new Among("s", -1, 3)
  };

  private static final Among[] a_4 = {
    new Among("end", -1, 1),
    new Among("ig", -1, 2),
    new Among("ing", -1, 1),
    new Among("lijk", -1, 3),
    new Among("baar", -1, 4),
    new Among("bar", -1, 5)
  };

  private static final Among[] a_5 = {
    new Among("aa", -1, -1),
    new Among("ee", -1, -1),
    new Among("oo", -1, -1),
    new Among("uu", -1, -1)
  };

  private static final char[] g_v = {17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128};

  private static final char[] g_v_I = {
    1, 0, 0, 17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128
  };

  private static final char[] g_v_j = {17, 67, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128};

  private int I_p2;
  private int I_p1;
  private boolean B_e_found;

  private boolean r_prelude() {
    int among_var;
    int v_1 = cursor;
    while (true) {
      int v_2 = cursor;
      lab0:
      {
        bra = cursor;
        among_var = find_among(a_0);
        ket = cursor;
        switch (among_var) {
          case 1:
            slice_from("a");
            break;
          case 2:
            slice_from("e");
            break;
          case 3:
            slice_from("i");
            break;
          case 4:
            slice_from("o");
            break;
          case 5:
            slice_from("u");
            break;
          case 6:
            if (cursor >= limit) {
              break lab0;
            }
            cursor++;
            break;
        }
        continue;
      }
      cursor = v_2;
      break;
    }
    cursor = v_1;
    int v_3 = cursor;
    lab1:
    {
      bra = cursor;
      if (!(eq_s("y"))) {
        cursor = v_3;
        break lab1;
      }
      ket = cursor;
      slice_from("Y");
    }
    while (true) {
      int v_4 = cursor;
      lab2:
      {
        if (!go_out_grouping(g_v, 97, 232)) {
          break lab2;
        }
        cursor++;
        int v_5 = cursor;
        lab3:
        {
          bra = cursor;
          lab4:
          {
            int v_6 = cursor;
            lab5:
            {
              if (!(eq_s("i"))) {
                break lab5;
              }
              ket = cursor;
              int v_7 = cursor;
              lab6:
              {
                if (!(in_grouping(g_v, 97, 232))) {
                  break lab6;
                }
                slice_from("I");
              }
              cursor = v_7;
              break lab4;
            }
            cursor = v_6;
            if (!(eq_s("y"))) {
              cursor = v_5;
              break lab3;
            }
            ket = cursor;
            slice_from("Y");
          }
        }
        continue;
      }
      cursor = v_4;
      break;
    }
    return true;
  }

  private boolean r_mark_regions() {
    int I_x;
    I_p1 = limit;
    I_p2 = limit;
    int v_1 = cursor;
    {
      int c = cursor + 3;
      if (c > limit) {
        return false;
      }
      cursor = c;
    }
    I_x = cursor;
    cursor = v_1;
    if (!go_out_grouping(g_v, 97, 232)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 232)) {
      return false;
    }
    cursor++;
    I_p1 = cursor;
    lab0:
    {
      if (I_p1 >= I_x) {
        break lab0;
      }
      I_p1 = I_x;
    }
    if (!go_out_grouping(g_v, 97, 232)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 232)) {
      return false;
    }
    cursor++;
    I_p2 = cursor;
    return true;
  }

  private boolean r_postlude() {
    int among_var;
    while (true) {
      int v_1 = cursor;
      lab0:
      {
        bra = cursor;
        among_var = find_among(a_1);
        ket = cursor;
        switch (among_var) {
          case 1:
            slice_from("y");
            break;
          case 2:
            slice_from("i");
            break;
          case 3:
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
    return true;
  }

  private boolean r_R1() {
    return I_p1 <= cursor;
  }

  private boolean r_R2() {
    return I_p2 <= cursor;
  }

  private boolean r_undouble() {
    int v_1 = limit - cursor;
    if (find_among_b(a_2) == 0) {
      return false;
    }
    cursor = limit - v_1;
    ket = cursor;
    if (cursor <= limit_backward) {
      return false;
    }
    cursor--;
    bra = cursor;
    slice_del();
    return true;
  }

  private boolean r_e_ending() {
    B_e_found = false;
    ket = cursor;
    if (!(eq_s_b("e"))) {
      return false;
    }
    bra = cursor;
    if (!r_R1()) {
      return false;
    }
    int v_1 = limit - cursor;
    if (!(out_grouping_b(g_v, 97, 232))) {
      return false;
    }
    cursor = limit - v_1;
    slice_del();
    B_e_found = true;
    return r_undouble();
  }

  private boolean r_en_ending() {
    if (!r_R1()) {
      return false;
    }
    int v_1 = limit - cursor;
    if (!(out_grouping_b(g_v, 97, 232))) {
      return false;
    }
    cursor = limit - v_1;
    {
      int v_2 = limit - cursor;
      lab0:
      {
        if (!(eq_s_b("gem"))) {
          break lab0;
        }
        return false;
      }
      cursor = limit - v_2;
    }
    slice_del();
    return r_undouble();
  }

  private boolean r_standard_suffix() {
    int among_var;
    int v_1 = limit - cursor;
    lab0:
    {
      ket = cursor;
      among_var = find_among_b(a_3);
      if (among_var == 0) {
        break lab0;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          if (!r_R1()) {
            break lab0;
          }
          slice_from("heid");
          break;
        case 2:
          if (!r_en_ending()) {
            break lab0;
          }
          break;
        case 3:
          if (!r_R1()) {
            break lab0;
          }
          if (!(out_grouping_b(g_v_j, 97, 232))) {
            break lab0;
          }
          slice_del();
          break;
      }
    }
    cursor = limit - v_1;
    int v_2 = limit - cursor;
    r_e_ending();
    cursor = limit - v_2;
    int v_3 = limit - cursor;
    lab1:
    {
      ket = cursor;
      if (!(eq_s_b("heid"))) {
        break lab1;
      }
      bra = cursor;
      if (!r_R2()) {
        break lab1;
      }
      {
        int v_4 = limit - cursor;
        lab2:
        {
          if (!(eq_s_b("c"))) {
            break lab2;
          }
          break lab1;
        }
        cursor = limit - v_4;
      }
      slice_del();
      ket = cursor;
      if (!(eq_s_b("en"))) {
        break lab1;
      }
      bra = cursor;
      if (!r_en_ending()) {
        break lab1;
      }
    }
    cursor = limit - v_3;
    int v_5 = limit - cursor;
    lab3:
    {
      ket = cursor;
      among_var = find_among_b(a_4);
      if (among_var == 0) {
        break lab3;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          if (!r_R2()) {
            break lab3;
          }
          slice_del();
          lab4:
          {
            int v_6 = limit - cursor;
            lab5:
            {
              ket = cursor;
              if (!(eq_s_b("ig"))) {
                break lab5;
              }
              bra = cursor;
              if (!r_R2()) {
                break lab5;
              }
              {
                int v_7 = limit - cursor;
                lab6:
                {
                  if (!(eq_s_b("e"))) {
                    break lab6;
                  }
                  break lab5;
                }
                cursor = limit - v_7;
              }
              slice_del();
              break lab4;
            }
            cursor = limit - v_6;
            if (!r_undouble()) {
              break lab3;
            }
          }
          break;
        case 2:
          if (!r_R2()) {
            break lab3;
          }
          {
            int v_8 = limit - cursor;
            lab7:
            {
              if (!(eq_s_b("e"))) {
                break lab7;
              }
              break lab3;
            }
            cursor = limit - v_8;
          }
          slice_del();
          break;
        case 3:
          if (!r_R2()) {
            break lab3;
          }
          slice_del();
          if (!r_e_ending()) {
            break lab3;
          }
          break;
        case 4:
          if (!r_R2()) {
            break lab3;
          }
          slice_del();
          break;
        case 5:
          if (!r_R2()) {
            break lab3;
          }
          if (!B_e_found) {
            break lab3;
          }
          slice_del();
          break;
      }
    }
    cursor = limit - v_5;
    int v_9 = limit - cursor;
    lab8:
    {
      if (!(out_grouping_b(g_v_I, 73, 232))) {
        break lab8;
      }
      int v_10 = limit - cursor;
      if (find_among_b(a_5) == 0) {
        break lab8;
      }
      if (!(out_grouping_b(g_v, 97, 232))) {
        break lab8;
      }
      cursor = limit - v_10;
      ket = cursor;
      if (cursor <= limit_backward) {
        break lab8;
      }
      cursor--;
      bra = cursor;
      slice_del();
    }
    cursor = limit - v_9;
    return true;
  }

  @Override
  public boolean stem() {
    int v_1 = cursor;
    r_prelude();
    cursor = v_1;
    int v_2 = cursor;
    r_mark_regions();
    cursor = v_2;
    limit_backward = cursor;
    cursor = limit;
    r_standard_suffix();
    cursor = limit_backward;
    int v_3 = cursor;
    r_postlude();
    cursor = v_3;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Dutch_porterStemmer;
  }

  @Override
  public int hashCode() {
    return Dutch_porterStemmer.class.getName().hashCode();
  }
}
