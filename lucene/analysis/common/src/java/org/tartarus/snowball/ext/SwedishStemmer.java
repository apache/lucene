// Generated from swedish.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from swedish.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class SwedishStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("fab", -1, -1),
    new Among("h", -1, -1),
    new Among("pak", -1, -1),
    new Among("rak", -1, -1),
    new Among("stak", -1, -1),
    new Among("kom", -1, -1),
    new Among("iet", -1, -1),
    new Among("cit", -1, -1),
    new Among("dit", -1, -1),
    new Among("alit", -1, -1),
    new Among("ilit", -1, -1),
    new Among("mit", -1, -1),
    new Among("nit", -1, -1),
    new Among("pit", -1, -1),
    new Among("rit", -1, -1),
    new Among("sit", -1, -1),
    new Among("tit", -1, -1),
    new Among("uit", -1, -1),
    new Among("ivit", -1, -1),
    new Among("kvit", -1, -1),
    new Among("xit", -1, -1)
  };

  private static final Among[] a_1 = {
    new Among("a", -1, 1),
    new Among("arna", 0, 1),
    new Among("erna", 0, 1),
    new Among("heterna", 2, 1),
    new Among("orna", 0, 1),
    new Among("ad", -1, 1),
    new Among("e", -1, 1),
    new Among("ade", 6, 1),
    new Among("ande", 6, 1),
    new Among("arne", 6, 1),
    new Among("are", 6, 1),
    new Among("aste", 6, 1),
    new Among("en", -1, 1),
    new Among("anden", 12, 1),
    new Among("aren", 12, 1),
    new Among("heten", 12, 1),
    new Among("ern", -1, 1),
    new Among("ar", -1, 1),
    new Among("er", -1, 1),
    new Among("heter", 18, 1),
    new Among("or", -1, 1),
    new Among("s", -1, 2),
    new Among("as", 21, 1),
    new Among("arnas", 22, 1),
    new Among("ernas", 22, 1),
    new Among("ornas", 22, 1),
    new Among("es", 21, 1),
    new Among("ades", 26, 1),
    new Among("andes", 26, 1),
    new Among("ens", 21, 1),
    new Among("arens", 29, 1),
    new Among("hetens", 29, 1),
    new Among("erns", 21, 1),
    new Among("at", -1, 1),
    new Among("et", -1, 3),
    new Among("andet", 34, 1),
    new Among("het", 34, 1),
    new Among("ast", -1, 1)
  };

  private static final Among[] a_2 = {
    new Among("dd", -1, -1),
    new Among("gd", -1, -1),
    new Among("nn", -1, -1),
    new Among("dt", -1, -1),
    new Among("gt", -1, -1),
    new Among("kt", -1, -1),
    new Among("tt", -1, -1)
  };

  private static final Among[] a_3 = {
    new Among("ig", -1, 1),
    new Among("lig", 0, 1),
    new Among("els", -1, 1),
    new Among("fullt", -1, 3),
    new Among("\u00F6st", -1, 2)
  };

  private static final char[] g_v = {17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 32};

  private static final char[] g_s_ending = {119, 127, 149};

  private static final char[] g_ost_ending = {173, 58};

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
    if (!go_out_grouping(g_v, 97, 246)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 246)) {
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

  private boolean r_et_condition() {
    int v_1 = limit - cursor;
    if (!(out_grouping_b(g_v, 97, 246))) {
      return false;
    }
    if (!(in_grouping_b(g_v, 97, 246))) {
      return false;
    }
    lab0:
    {
      if (cursor > limit_backward) {
        break lab0;
      }
      return false;
    }
    cursor = limit - v_1;
    {
      int v_2 = limit - cursor;
      lab1:
      {
        if (find_among_b(a_0) == 0) {
          break lab1;
        }
        return false;
      }
      cursor = limit - v_2;
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
        lab0:
        {
          int v_2 = limit - cursor;
          lab1:
          {
            if (!(eq_s_b("et"))) {
              break lab1;
            }
            if (!r_et_condition()) {
              break lab1;
            }
            bra = cursor;
            break lab0;
          }
          cursor = limit - v_2;
          if (!(in_grouping_b(g_s_ending, 98, 121))) {
            return false;
          }
        }
        slice_del();
        break;
      case 3:
        if (!r_et_condition()) {
          return false;
        }
        slice_del();
        break;
    }
    return true;
  }

  private boolean r_consonant_pair() {
    if (cursor < I_p1) {
      return false;
    }
    int v_1 = limit_backward;
    limit_backward = I_p1;
    int v_2 = limit - cursor;
    if (find_among_b(a_2) == 0) {
      limit_backward = v_1;
      return false;
    }
    cursor = limit - v_2;
    ket = cursor;
    if (cursor <= limit_backward) {
      limit_backward = v_1;
      return false;
    }
    cursor--;
    bra = cursor;
    slice_del();
    limit_backward = v_1;
    return true;
  }

  private boolean r_other_suffix() {
    int among_var;
    if (cursor < I_p1) {
      return false;
    }
    int v_1 = limit_backward;
    limit_backward = I_p1;
    ket = cursor;
    among_var = find_among_b(a_3);
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
        if (!(in_grouping_b(g_ost_ending, 105, 118))) {
          return false;
        }
        slice_from("\u00F6s");
        break;
      case 3:
        slice_from("full");
        break;
    }
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
    return o instanceof SwedishStemmer;
  }

  @Override
  public int hashCode() {
    return SwedishStemmer.class.getName().hashCode();
  }
}
