// Generated from german.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from german.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class GermanStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("", -1, 5),
    new Among("ae", 0, 2),
    new Among("oe", 0, 3),
    new Among("qu", 0, -1),
    new Among("ue", 0, 4),
    new Among("\u00DF", 0, 1)
  };

  private static final Among[] a_1 = {
    new Among("", -1, 5),
    new Among("U", 0, 2),
    new Among("Y", 0, 1),
    new Among("\u00E4", 0, 3),
    new Among("\u00F6", 0, 4),
    new Among("\u00FC", 0, 2)
  };

  private static final Among[] a_2 = {
    new Among("e", -1, 3),
    new Among("em", -1, 1),
    new Among("en", -1, 3),
    new Among("erinnen", 2, 2),
    new Among("erin", -1, 2),
    new Among("ln", -1, 5),
    new Among("ern", -1, 2),
    new Among("er", -1, 2),
    new Among("s", -1, 4),
    new Among("es", 8, 3),
    new Among("lns", 8, 5)
  };

  private static final Among[] a_3 = {
    new Among("tick", -1, -1),
    new Among("plan", -1, -1),
    new Among("geordn", -1, -1),
    new Among("intern", -1, -1),
    new Among("tr", -1, -1)
  };

  private static final Among[] a_4 = {
    new Among("en", -1, 1),
    new Among("er", -1, 1),
    new Among("et", -1, 3),
    new Among("st", -1, 2),
    new Among("est", 3, 1)
  };

  private static final Among[] a_5 = {new Among("ig", -1, 1), new Among("lich", -1, 1)};

  private static final Among[] a_6 = {
    new Among("end", -1, 1),
    new Among("ig", -1, 2),
    new Among("ung", -1, 1),
    new Among("lich", -1, 3),
    new Among("isch", -1, 2),
    new Among("ik", -1, 2),
    new Among("heit", -1, 3),
    new Among("keit", -1, 4)
  };

  private static final char[] g_v = {
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 32, 8
  };

  private static final char[] g_et_ending = {
    1, 128, 198, 227, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128
  };

  private static final char[] g_s_ending = {117, 30, 5};

  private static final char[] g_st_ending = {117, 30, 4};

  private int I_p2;
  private int I_p1;

  private boolean r_prelude() {
    int among_var;
    int v_1 = cursor;
    while (true) {
      int v_2 = cursor;
      lab0:
      {
        golab1:
        while (true) {
          int v_3 = cursor;
          lab2:
          {
            if (!(in_grouping(g_v, 97, 252))) {
              break lab2;
            }
            bra = cursor;
            lab3:
            {
              int v_4 = cursor;
              lab4:
              {
                if (!(eq_s("u"))) {
                  break lab4;
                }
                ket = cursor;
                if (!(in_grouping(g_v, 97, 252))) {
                  break lab4;
                }
                slice_from("U");
                break lab3;
              }
              cursor = v_4;
              if (!(eq_s("y"))) {
                break lab2;
              }
              ket = cursor;
              if (!(in_grouping(g_v, 97, 252))) {
                break lab2;
              }
              slice_from("Y");
            }
            cursor = v_3;
            break golab1;
          }
          cursor = v_3;
          if (cursor >= limit) {
            break lab0;
          }
          cursor++;
        }
        continue;
      }
      cursor = v_2;
      break;
    }
    cursor = v_1;
    while (true) {
      int v_5 = cursor;
      lab5:
      {
        bra = cursor;
        among_var = find_among(a_0);
        ket = cursor;
        switch (among_var) {
          case 1:
            slice_from("ss");
            break;
          case 2:
            slice_from("\u00E4");
            break;
          case 3:
            slice_from("\u00F6");
            break;
          case 4:
            slice_from("\u00FC");
            break;
          case 5:
            if (cursor >= limit) {
              break lab5;
            }
            cursor++;
            break;
        }
        continue;
      }
      cursor = v_5;
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
    if (!go_out_grouping(g_v, 97, 252)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 252)) {
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
    if (!go_out_grouping(g_v, 97, 252)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 252)) {
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
            slice_from("u");
            break;
          case 3:
            slice_from("a");
            break;
          case 4:
            slice_from("o");
            break;
          case 5:
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

  private boolean r_standard_suffix() {
    int among_var;
    int v_1 = limit - cursor;
    lab0:
    {
      ket = cursor;
      among_var = find_among_b(a_2);
      if (among_var == 0) {
        break lab0;
      }
      bra = cursor;
      if (!r_R1()) {
        break lab0;
      }
      switch (among_var) {
        case 1:
          {
            int v_2 = limit - cursor;
            lab1:
            {
              if (!(eq_s_b("syst"))) {
                break lab1;
              }
              break lab0;
            }
            cursor = limit - v_2;
          }
          slice_del();
          break;
        case 2:
          slice_del();
          break;
        case 3:
          slice_del();
          int v_3 = limit - cursor;
          lab2:
          {
            ket = cursor;
            if (!(eq_s_b("s"))) {
              cursor = limit - v_3;
              break lab2;
            }
            bra = cursor;
            if (!(eq_s_b("nis"))) {
              cursor = limit - v_3;
              break lab2;
            }
            slice_del();
          }
          break;
        case 4:
          if (!(in_grouping_b(g_s_ending, 98, 116))) {
            break lab0;
          }
          slice_del();
          break;
        case 5:
          slice_from("l");
          break;
      }
    }
    cursor = limit - v_1;
    int v_4 = limit - cursor;
    lab3:
    {
      ket = cursor;
      among_var = find_among_b(a_4);
      if (among_var == 0) {
        break lab3;
      }
      bra = cursor;
      if (!r_R1()) {
        break lab3;
      }
      switch (among_var) {
        case 1:
          slice_del();
          break;
        case 2:
          if (!(in_grouping_b(g_st_ending, 98, 116))) {
            break lab3;
          }
          {
            int c = cursor - 3;
            if (c < limit_backward) {
              break lab3;
            }
            cursor = c;
          }
          slice_del();
          break;
        case 3:
          int v_5 = limit - cursor;
          if (!(in_grouping_b(g_et_ending, 85, 228))) {
            break lab3;
          }
          cursor = limit - v_5;
          {
            int v_6 = limit - cursor;
            lab4:
            {
              if (find_among_b(a_3) == 0) {
                break lab4;
              }
              break lab3;
            }
            cursor = limit - v_6;
          }
          slice_del();
          break;
      }
    }
    cursor = limit - v_4;
    int v_7 = limit - cursor;
    lab5:
    {
      ket = cursor;
      among_var = find_among_b(a_6);
      if (among_var == 0) {
        break lab5;
      }
      bra = cursor;
      if (!r_R2()) {
        break lab5;
      }
      switch (among_var) {
        case 1:
          slice_del();
          int v_8 = limit - cursor;
          lab6:
          {
            ket = cursor;
            if (!(eq_s_b("ig"))) {
              cursor = limit - v_8;
              break lab6;
            }
            bra = cursor;
            {
              int v_9 = limit - cursor;
              lab7:
              {
                if (!(eq_s_b("e"))) {
                  break lab7;
                }
                cursor = limit - v_8;
                break lab6;
              }
              cursor = limit - v_9;
            }
            if (!r_R2()) {
              cursor = limit - v_8;
              break lab6;
            }
            slice_del();
          }
          break;
        case 2:
          {
            int v_10 = limit - cursor;
            lab8:
            {
              if (!(eq_s_b("e"))) {
                break lab8;
              }
              break lab5;
            }
            cursor = limit - v_10;
          }
          slice_del();
          break;
        case 3:
          slice_del();
          int v_11 = limit - cursor;
          lab9:
          {
            ket = cursor;
            lab10:
            {
              int v_12 = limit - cursor;
              lab11:
              {
                if (!(eq_s_b("er"))) {
                  break lab11;
                }
                break lab10;
              }
              cursor = limit - v_12;
              if (!(eq_s_b("en"))) {
                cursor = limit - v_11;
                break lab9;
              }
            }
            bra = cursor;
            if (!r_R1()) {
              cursor = limit - v_11;
              break lab9;
            }
            slice_del();
          }
          break;
        case 4:
          slice_del();
          int v_13 = limit - cursor;
          lab12:
          {
            ket = cursor;
            if (find_among_b(a_5) == 0) {
              cursor = limit - v_13;
              break lab12;
            }
            bra = cursor;
            if (!r_R2()) {
              cursor = limit - v_13;
              break lab12;
            }
            slice_del();
          }
          break;
      }
    }
    cursor = limit - v_7;
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
    return o instanceof GermanStemmer;
  }

  @Override
  public int hashCode() {
    return GermanStemmer.class.getName().hashCode();
  }
}
