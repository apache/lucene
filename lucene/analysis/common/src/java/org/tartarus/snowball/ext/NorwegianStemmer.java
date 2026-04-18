// Generated from norwegian.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from norwegian.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class NorwegianStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("", -1, 1),
    new Among("ind", 0, -1),
    new Among("kk", 0, -1),
    new Among("nk", 0, -1),
    new Among("amm", 0, -1),
    new Among("omm", 0, -1),
    new Among("kap", 0, -1),
    new Among("skap", 6, 1),
    new Among("pp", 0, -1),
    new Among("lt", 0, -1),
    new Among("ast", 0, -1),
    new Among("\u00F8st", 0, -1),
    new Among("v", 0, -1),
    new Among("hav", 12, 1),
    new Among("giv", 12, 1)
  };

  private static final Among[] a_1 = {
    new Among("a", -1, 1),
    new Among("e", -1, 1),
    new Among("ede", 1, 1),
    new Among("ande", 1, 1),
    new Among("ende", 1, 1),
    new Among("ane", 1, 1),
    new Among("ene", 1, 1),
    new Among("hetene", 6, 1),
    new Among("erte", 1, 4),
    new Among("en", -1, 1),
    new Among("heten", 9, 1),
    new Among("ar", -1, 1),
    new Among("er", -1, 1),
    new Among("heter", 12, 1),
    new Among("s", -1, 3),
    new Among("as", 14, 1),
    new Among("es", 14, 1),
    new Among("edes", 16, 1),
    new Among("endes", 16, 1),
    new Among("enes", 16, 1),
    new Among("hetenes", 19, 1),
    new Among("ens", 14, 1),
    new Among("hetens", 21, 1),
    new Among("ers", 14, 2),
    new Among("ets", 14, 1),
    new Among("et", -1, 1),
    new Among("het", 25, 1),
    new Among("ert", -1, 4),
    new Among("ast", -1, 1)
  };

  private static final Among[] a_2 = {new Among("dt", -1, -1), new Among("vt", -1, -1)};

  private static final Among[] a_3 = {
    new Among("leg", -1, 1),
    new Among("eleg", 0, 1),
    new Among("ig", -1, 1),
    new Among("eig", 2, 1),
    new Among("lig", 2, 1),
    new Among("elig", 4, 1),
    new Among("els", -1, 1),
    new Among("lov", -1, 1),
    new Among("elov", 7, 1),
    new Among("slov", 7, 1),
    new Among("hetslov", 9, 1)
  };

  private static final char[] g_v = {17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 48, 2, 142};

  private static final char[] g_s_ending = {119, 125, 148, 1};

  private int I_p1;

  private boolean r_mark_regions() {
    int I_x;
    I_p1 = limit;
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
    if (!go_out_grouping(g_v, 97, 248)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 248)) {
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
    return true;
  }

  private boolean r_main_suffix() {
    int among_var;
    if (cursor < I_p1) {
      return false;
    }
    int v_1 = limit_backward;
    limit_backward = I_p1;
    ket = cursor;
    among_var = find_among_b(a_1);
    if (among_var == 0) {
      limit_backward = v_1;
      return false;
    }
    bra = cursor;
    limit_backward = v_1;
    switch (among_var) {
      case 1:
        slice_del();
        break;
      case 2:
        among_var = find_among_b(a_0);
        switch (among_var) {
          case 1:
            slice_del();
            break;
        }
        break;
      case 3:
        lab0:
        {
          int v_2 = limit - cursor;
          lab1:
          {
            if (!(in_grouping_b(g_s_ending, 98, 122))) {
              break lab1;
            }
            break lab0;
          }
          cursor = limit - v_2;
          lab2:
          {
            if (!(eq_s_b("r"))) {
              break lab2;
            }
            {
              int v_3 = limit - cursor;
              lab3:
              {
                if (!(eq_s_b("e"))) {
                  break lab3;
                }
                break lab2;
              }
              cursor = limit - v_3;
            }
            break lab0;
          }
          cursor = limit - v_2;
          if (!(eq_s_b("k"))) {
            return false;
          }
          if (!(out_grouping_b(g_v, 97, 248))) {
            return false;
          }
        }
        slice_del();
        break;
      case 4:
        slice_from("er");
        break;
    }
    return true;
  }

  private boolean r_consonant_pair() {
    int v_1 = limit - cursor;
    if (cursor < I_p1) {
      return false;
    }
    int v_2 = limit_backward;
    limit_backward = I_p1;
    ket = cursor;
    if (find_among_b(a_2) == 0) {
      limit_backward = v_2;
      return false;
    }
    bra = cursor;
    limit_backward = v_2;
    cursor = limit - v_1;
    if (cursor <= limit_backward) {
      return false;
    }
    cursor--;
    bra = cursor;
    slice_del();
    return true;
  }

  private boolean r_other_suffix() {
    if (cursor < I_p1) {
      return false;
    }
    int v_1 = limit_backward;
    limit_backward = I_p1;
    ket = cursor;
    if (find_among_b(a_3) == 0) {
      limit_backward = v_1;
      return false;
    }
    bra = cursor;
    limit_backward = v_1;
    slice_del();
    return true;
  }

  @Override
  public boolean stem() {
    int v_1 = cursor;
    r_mark_regions();
    cursor = v_1;
    limit_backward = cursor;
    cursor = limit;
    int v_2 = limit - cursor;
    r_main_suffix();
    cursor = limit - v_2;
    int v_3 = limit - cursor;
    r_consonant_pair();
    cursor = limit - v_3;
    int v_4 = limit - cursor;
    r_other_suffix();
    cursor = limit - v_4;
    cursor = limit_backward;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof NorwegianStemmer;
  }

  @Override
  public int hashCode() {
    return NorwegianStemmer.class.getName().hashCode();
  }
}
