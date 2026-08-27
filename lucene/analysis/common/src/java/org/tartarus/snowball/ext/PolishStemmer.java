// Generated from polish.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from polish.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class PolishStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;
  private static final java.lang.invoke.MethodHandles.Lookup methodObject =
      java.lang.invoke.MethodHandles.lookup();

  private static final Among[] a_0 = {
    new Among("by\u015Bcie", -1, 1),
    new Among("bym", -1, 1),
    new Among("by", -1, 1),
    new Among("by\u015Bmy", -1, 1),
    new Among("by\u015B", -1, 1)
  };

  private static final Among[] a_1 = {
    new Among("\u0105c", -1, 1),
    new Among("aj\u0105c", 0, 1),
    new Among("sz\u0105c", 0, 2),
    new Among("sz", -1, 1),
    new Among("iejsz", 3, 1)
  };

  private static final Among[] a_2 = {
    new Among("a", -1, 1, "r_R1", methodObject),
    new Among("\u0105ca", 0, 1),
    new Among("aj\u0105ca", 1, 1),
    new Among("sz\u0105ca", 1, 2),
    new Among("ia", 0, 1, "r_R1", methodObject),
    new Among("sza", 0, 1),
    new Among("iejsza", 5, 1),
    new Among("a\u0142a", 0, 1),
    new Among("ia\u0142a", 7, 1),
    new Among("i\u0142a", 0, 1),
    new Among("\u0105c", -1, 1),
    new Among("aj\u0105c", 10, 1),
    new Among("e", -1, 1, "r_R1", methodObject),
    new Among("\u0105ce", 12, 1),
    new Among("aj\u0105ce", 13, 1),
    new Among("sz\u0105ce", 13, 2),
    new Among("ie", 12, 1, "r_R1", methodObject),
    new Among("cie", 16, 1),
    new Among("acie", 17, 1),
    new Among("ecie", 17, 1),
    new Among("icie", 17, 1),
    new Among("ajcie", 17, 1),
    new Among("li\u015Bcie", 17, 4),
    new Among("ali\u015Bcie", 22, 1),
    new Among("ieli\u015Bcie", 22, 1),
    new Among("ili\u015Bcie", 22, 1),
    new Among("\u0142y\u015Bcie", 17, 4),
    new Among("a\u0142y\u015Bcie", 26, 1),
    new Among("ia\u0142y\u015Bcie", 27, 1),
    new Among("i\u0142y\u015Bcie", 26, 1),
    new Among("sze", 12, 1),
    new Among("iejsze", 30, 1),
    new Among("ach", -1, 1, "r_R1", methodObject),
    new Among("iach", 32, 1, "r_R1", methodObject),
    new Among("ich", -1, 5),
    new Among("ych", -1, 5),
    new Among("i", -1, 1, "r_R1", methodObject),
    new Among("ali", 36, 1),
    new Among("ieli", 36, 1),
    new Among("ili", 36, 1),
    new Among("ami", 36, 1, "r_R1", methodObject),
    new Among("iami", 40, 1, "r_R1", methodObject),
    new Among("imi", 36, 5),
    new Among("ymi", 36, 5),
    new Among("owi", 36, 1, "r_R1", methodObject),
    new Among("iowi", 44, 1, "r_R1", methodObject),
    new Among("aj", -1, 1),
    new Among("ej", -1, 5),
    new Among("iej", 47, 5),
    new Among("am", -1, 1),
    new Among("a\u0142am", 49, 1),
    new Among("ia\u0142am", 50, 1),
    new Among("i\u0142am", 49, 1),
    new Among("em", -1, 1, "r_R1", methodObject),
    new Among("iem", 53, 1, "r_R1", methodObject),
    new Among("a\u0142em", 53, 1),
    new Among("ia\u0142em", 55, 1),
    new Among("i\u0142em", 53, 1),
    new Among("im", -1, 5),
    new Among("om", -1, 1, "r_R1", methodObject),
    new Among("iom", 59, 1, "r_R1", methodObject),
    new Among("ym", -1, 5),
    new Among("o", -1, 1, "r_R1", methodObject),
    new Among("ego", 62, 5),
    new Among("iego", 63, 5),
    new Among("a\u0142o", 62, 1),
    new Among("ia\u0142o", 65, 1),
    new Among("i\u0142o", 62, 1),
    new Among("u", -1, 1, "r_R1", methodObject),
    new Among("iu", 68, 1, "r_R1", methodObject),
    new Among("emu", 68, 5),
    new Among("iemu", 70, 5),
    new Among("\u00F3w", -1, 1, "r_R1", methodObject),
    new Among("y", -1, 5),
    new Among("amy", 73, 1),
    new Among("emy", 73, 1),
    new Among("imy", 73, 1),
    new Among("li\u015Bmy", 73, 4),
    new Among("ali\u015Bmy", 77, 1),
    new Among("ieli\u015Bmy", 77, 1),
    new Among("ili\u015Bmy", 77, 1),
    new Among("\u0142y\u015Bmy", 73, 4),
    new Among("a\u0142y\u015Bmy", 81, 1),
    new Among("ia\u0142y\u015Bmy", 82, 1),
    new Among("i\u0142y\u015Bmy", 81, 1),
    new Among("a\u0142y", 73, 1),
    new Among("ia\u0142y", 85, 1),
    new Among("i\u0142y", 73, 1),
    new Among("asz", -1, 1),
    new Among("esz", -1, 1),
    new Among("isz", -1, 1),
    new Among("\u0105", -1, 1, "r_R1", methodObject),
    new Among("\u0105c\u0105", 91, 1),
    new Among("aj\u0105c\u0105", 92, 1),
    new Among("sz\u0105c\u0105", 92, 2),
    new Among("i\u0105", 91, 1, "r_R1", methodObject),
    new Among("aj\u0105", 91, 1),
    new Among("sz\u0105", 91, 3),
    new Among("iejsz\u0105", 97, 1),
    new Among("a\u0107", -1, 1),
    new Among("ie\u0107", -1, 1),
    new Among("i\u0107", -1, 1),
    new Among("\u0105\u0107", -1, 1),
    new Among("a\u015B\u0107", -1, 1),
    new Among("e\u015B\u0107", -1, 1),
    new Among("\u0119", -1, 1),
    new Among("sz\u0119", 105, 2),
    new Among("a\u0142", -1, 1),
    new Among("ia\u0142", 107, 1),
    new Among("i\u0142", -1, 1),
    new Among("\u0142a\u015B", -1, 4),
    new Among("a\u0142a\u015B", 110, 1),
    new Among("ia\u0142a\u015B", 111, 1),
    new Among("i\u0142a\u015B", 110, 1),
    new Among("\u0142e\u015B", -1, 4),
    new Among("a\u0142e\u015B", 114, 1),
    new Among("ia\u0142e\u015B", 115, 1),
    new Among("i\u0142e\u015B", 114, 1)
  };

  private static final Among[] a_3 = {
    new Among("\u0107", -1, 1),
    new Among("\u0144", -1, 2),
    new Among("\u015B", -1, 3),
    new Among("\u017A", -1, 4)
  };

  private static final char[] g_v = {
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 16, 0, 0, 1
  };

  private int I_p1;

  private boolean r_mark_regions() {
    I_p1 = limit;
    if (!go_out_grouping(g_v, 97, 281)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_v, 97, 281)) {
      return false;
    }
    cursor++;
    I_p1 = cursor;
    return true;
  }

  private boolean r_R1() {
    return I_p1 <= cursor;
  }

  private boolean r_remove_endings() {
    int among_var;
    int v_1 = limit - cursor;
    lab0:
    {
      if (cursor < I_p1) {
        break lab0;
      }
      int v_2 = limit_backward;
      limit_backward = I_p1;
      ket = cursor;
      if (find_among_b(a_0) == 0) {
        limit_backward = v_2;
        break lab0;
      }
      bra = cursor;
      limit_backward = v_2;
      slice_del();
    }
    cursor = limit - v_1;
    ket = cursor;
    among_var = find_among_b(a_2);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        slice_del();
        break;
      case 2:
        slice_from("s");
        break;
      case 3:
        lab1:
        {
          int v_3 = limit - cursor;
          lab2:
          {
            int v_4 = limit - cursor;
            if (!r_R1()) {
              break lab2;
            }
            cursor = limit - v_4;
            slice_del();
            break lab1;
          }
          cursor = limit - v_3;
          slice_from("s");
        }
        break;
      case 4:
        slice_from("\u0142");
        break;
      case 5:
        slice_del();
        int v_5 = limit - cursor;
        lab3:
        {
          ket = cursor;
          among_var = find_among_b(a_1);
          if (among_var == 0) {
            cursor = limit - v_5;
            break lab3;
          }
          bra = cursor;
          switch (among_var) {
            case 1:
              slice_del();
              break;
            case 2:
              slice_from("s");
              break;
          }
        }
        break;
    }
    return true;
  }

  private boolean r_normalize_consonant() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_3);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    lab0:
    {
      if (cursor > limit_backward) {
        break lab0;
      }
      return false;
    }
    switch (among_var) {
      case 1:
        slice_from("c");
        break;
      case 2:
        slice_from("n");
        break;
      case 3:
        slice_from("s");
        break;
      case 4:
        slice_from("z");
        break;
    }
    return true;
  }

  @Override
  public boolean stem() {
    int v_1 = cursor;
    r_mark_regions();
    cursor = v_1;
    lab0:
    {
      int v_2 = cursor;
      lab1:
      {
        {
          int c = cursor + 2;
          if (c > limit) {
            break lab1;
          }
          cursor = c;
        }
        limit_backward = cursor;
        cursor = limit;
        if (!r_remove_endings()) {
          break lab1;
        }
        cursor = limit_backward;
        break lab0;
      }
      cursor = v_2;
      limit_backward = cursor;
      cursor = limit;
      if (!r_normalize_consonant()) {
        return false;
      }
      cursor = limit_backward;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PolishStemmer;
  }

  @Override
  public int hashCode() {
    return PolishStemmer.class.getName().hashCode();
  }
}
