// Generated from yiddish.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from yiddish.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class YiddishStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("\u05D5\u05D5", -1, 1),
    new Among("\u05D5\u05D9", -1, 2),
    new Among("\u05D9\u05D9", -1, 3),
    new Among("\u05DA", -1, 4),
    new Among("\u05DD", -1, 5),
    new Among("\u05DF", -1, 6),
    new Among("\u05E3", -1, 7),
    new Among("\u05E5", -1, 8)
  };

  private static final Among[] a_1 = {
    new Among("\u05D0\u05D3\u05D5\u05E8\u05DB", -1, 1),
    new Among("\u05D0\u05D4\u05D9\u05E0", -1, 1),
    new Among("\u05D0\u05D4\u05E2\u05E8", -1, 1),
    new Among("\u05D0\u05D4\u05F2\u05DE", -1, 1),
    new Among("\u05D0\u05D5\u05DE", -1, 1),
    new Among("\u05D0\u05D5\u05E0\u05D8\u05E2\u05E8", -1, 1),
    new Among("\u05D0\u05D9\u05D1\u05E2\u05E8", -1, 1),
    new Among("\u05D0\u05E0", -1, 1),
    new Among("\u05D0\u05E0\u05D8", 7, 1),
    new Among("\u05D0\u05E0\u05D8\u05E7\u05E2\u05D2\u05E0", 8, 1),
    new Among("\u05D0\u05E0\u05D9\u05D3\u05E2\u05E8", 7, 1),
    new Among("\u05D0\u05E4", -1, 1),
    new Among("\u05D0\u05E4\u05D9\u05E8", 11, 1),
    new Among("\u05D0\u05E7\u05E2\u05D2\u05E0", -1, 1),
    new Among("\u05D0\u05E8\u05D0\u05E4", -1, 1),
    new Among("\u05D0\u05E8\u05D5\u05DE", -1, 1),
    new Among("\u05D0\u05E8\u05D5\u05E0\u05D8\u05E2\u05E8", -1, 1),
    new Among("\u05D0\u05E8\u05D9\u05D1\u05E2\u05E8", -1, 1),
    new Among("\u05D0\u05E8\u05F1\u05E1", -1, 1),
    new Among("\u05D0\u05E8\u05F1\u05E4", -1, 1),
    new Among("\u05D0\u05E8\u05F2\u05E0", -1, 1),
    new Among("\u05D0\u05F0\u05E2\u05E7", -1, 1),
    new Among("\u05D0\u05F1\u05E1", -1, 1),
    new Among("\u05D0\u05F1\u05E4", -1, 1),
    new Among("\u05D0\u05F2\u05E0", -1, 1),
    new Among("\u05D1\u05D0", -1, 1),
    new Among("\u05D1\u05F2", -1, 1),
    new Among("\u05D3\u05D5\u05E8\u05DB", -1, 1),
    new Among("\u05D3\u05E2\u05E8", -1, 1),
    new Among("\u05DE\u05D9\u05D8", -1, 1),
    new Among("\u05E0\u05D0\u05DB", -1, 1),
    new Among("\u05E4\u05D0\u05E8", -1, 1),
    new Among("\u05E4\u05D0\u05E8\u05D1\u05F2", 31, 1),
    new Among("\u05E4\u05D0\u05E8\u05F1\u05E1", 31, 1),
    new Among("\u05E4\u05D5\u05E0\u05D0\u05E0\u05D3\u05E2\u05E8", -1, 1),
    new Among("\u05E6\u05D5", -1, 1),
    new Among("\u05E6\u05D5\u05D6\u05D0\u05DE\u05E2\u05E0", 35, 1),
    new Among("\u05E6\u05D5\u05E0\u05F1\u05E4", 35, 1),
    new Among("\u05E6\u05D5\u05E8\u05D9\u05E7", 35, 1),
    new Among("\u05E6\u05E2", -1, 1)
  };

  private static final Among[] a_2 = {
    new Among("\u05D3\u05D6\u05E9", -1, -1),
    new Among("\u05E9\u05D8\u05E8", -1, -1),
    new Among("\u05E9\u05D8\u05E9", -1, -1),
    new Among("\u05E9\u05E4\u05E8", -1, -1)
  };

  private static final Among[] a_3 = {
    new Among("\u05E7\u05DC\u05D9\u05D1", -1, 9),
    new Among("\u05E8\u05D9\u05D1", -1, 10),
    new Among("\u05D8\u05E8\u05D9\u05D1", 1, 7),
    new Among("\u05E9\u05E8\u05D9\u05D1", 1, 15),
    new Among("\u05D4\u05F1\u05D1", -1, 23),
    new Among("\u05E9\u05F0\u05D9\u05D2", -1, 12),
    new Among("\u05D2\u05D0\u05E0\u05D2", -1, 1),
    new Among("\u05D6\u05D5\u05E0\u05D2", -1, 18),
    new Among("\u05E9\u05DC\u05D5\u05E0\u05D2", -1, 21),
    new Among("\u05E6\u05F0\u05D5\u05E0\u05D2", -1, 20),
    new Among("\u05D1\u05F1\u05D2", -1, 22),
    new Among("\u05D1\u05D5\u05E0\u05D3", -1, 16),
    new Among("\u05F0\u05D9\u05D6", -1, 6),
    new Among("\u05D1\u05D9\u05D8", -1, 4),
    new Among("\u05DC\u05D9\u05D8", -1, 8),
    new Among("\u05DE\u05D9\u05D8", -1, 3),
    new Among("\u05E9\u05E0\u05D9\u05D8", -1, 14),
    new Among("\u05E0\u05D5\u05DE", -1, 2),
    new Among("\u05E9\u05D8\u05D0\u05E0", -1, 25),
    new Among("\u05D1\u05D9\u05E1", -1, 5),
    new Among("\u05E9\u05DE\u05D9\u05E1", -1, 13),
    new Among("\u05E8\u05D9\u05E1", -1, 11),
    new Among("\u05D8\u05E8\u05D5\u05E0\u05E7", -1, 19),
    new Among("\u05E4\u05D0\u05E8\u05DC\u05F1\u05E8", -1, 24),
    new Among("\u05E9\u05F0\u05F1\u05E8", -1, 26),
    new Among("\u05F0\u05D5\u05D8\u05E9", -1, 17)
  };

  private static final Among[] a_4 = {
    new Among("\u05D5\u05E0\u05D2", -1, 1),
    new Among("\u05E1\u05D8\u05D5", -1, 1),
    new Among("\u05D8", -1, 1),
    new Among("\u05D1\u05E8\u05D0\u05DB\u05D8", 2, 31),
    new Among("\u05E1\u05D8", 2, 1),
    new Among("\u05D9\u05E1\u05D8", 4, 33),
    new Among("\u05E2\u05D8", 2, 1),
    new Among("\u05E9\u05D0\u05E4\u05D8", 2, 1),
    new Among("\u05D4\u05F2\u05D8", 2, 1),
    new Among("\u05E7\u05F2\u05D8", 2, 1),
    new Among("\u05D9\u05E7\u05F2\u05D8", 9, 1),
    new Among("\u05DC\u05E2\u05DB", -1, 1),
    new Among("\u05E2\u05DC\u05E2\u05DB", 11, 1),
    new Among("\u05D9\u05D6\u05DE", -1, 1),
    new Among("\u05D9\u05DE", -1, 1),
    new Among("\u05E2\u05DE", -1, 1),
    new Among("\u05E2\u05E0\u05E2\u05DE", 15, 3),
    new Among("\u05D8\u05E2\u05E0\u05E2\u05DE", 16, 4),
    new Among("\u05E0", -1, 1),
    new Among("\u05E7\u05DC\u05D9\u05D1\u05E0", 18, 14),
    new Among("\u05E8\u05D9\u05D1\u05E0", 18, 15),
    new Among("\u05D8\u05E8\u05D9\u05D1\u05E0", 20, 12),
    new Among("\u05E9\u05E8\u05D9\u05D1\u05E0", 20, 7),
    new Among("\u05D4\u05F1\u05D1\u05E0", 18, 27),
    new Among("\u05E9\u05F0\u05D9\u05D2\u05E0", 18, 17),
    new Among("\u05D6\u05D5\u05E0\u05D2\u05E0", 18, 22),
    new Among("\u05E9\u05DC\u05D5\u05E0\u05D2\u05E0", 18, 25),
    new Among("\u05E6\u05F0\u05D5\u05E0\u05D2\u05E0", 18, 24),
    new Among("\u05D1\u05F1\u05D2\u05E0", 18, 26),
    new Among("\u05D1\u05D5\u05E0\u05D3\u05E0", 18, 20),
    new Among("\u05F0\u05D9\u05D6\u05E0", 18, 11),
    new Among("\u05D8\u05E0", 18, 4),
    new Among("GE\u05D1\u05D9\u05D8\u05E0", 31, 9),
    new Among("GE\u05DC\u05D9\u05D8\u05E0", 31, 13),
    new Among("GE\u05DE\u05D9\u05D8\u05E0", 31, 8),
    new Among("\u05E9\u05E0\u05D9\u05D8\u05E0", 31, 19),
    new Among("\u05E1\u05D8\u05E0", 31, 1),
    new Among("\u05D9\u05E1\u05D8\u05E0", 36, 1),
    new Among("\u05E2\u05D8\u05E0", 31, 1),
    new Among("GE\u05D1\u05D9\u05E1\u05E0", 18, 10),
    new Among("\u05E9\u05DE\u05D9\u05E1\u05E0", 18, 18),
    new Among("GE\u05E8\u05D9\u05E1\u05E0", 18, 16),
    new Among("\u05E2\u05E0", 18, 1),
    new Among("\u05D2\u05D0\u05E0\u05D2\u05E2\u05E0", 42, 5),
    new Among("\u05E2\u05DC\u05E2\u05E0", 42, 1),
    new Among("\u05E0\u05D5\u05DE\u05E2\u05E0", 42, 6),
    new Among("\u05D9\u05D6\u05DE\u05E2\u05E0", 42, 1),
    new Among("\u05E9\u05D8\u05D0\u05E0\u05E2\u05E0", 42, 29),
    new Among("\u05D8\u05E8\u05D5\u05E0\u05E7\u05E0", 18, 23),
    new Among("\u05E4\u05D0\u05E8\u05DC\u05F1\u05E8\u05E0", 18, 28),
    new Among("\u05E9\u05F0\u05F1\u05E8\u05E0", 18, 30),
    new Among("\u05F0\u05D5\u05D8\u05E9\u05E0", 18, 21),
    new Among("\u05D2\u05F2\u05E0", 18, 5),
    new Among("\u05E1", -1, 1),
    new Among("\u05D8\u05E1", 53, 4),
    new Among("\u05E2\u05D8\u05E1", 54, 1),
    new Among("\u05E0\u05E1", 53, 1),
    new Among("\u05D8\u05E0\u05E1", 56, 4),
    new Among("\u05E2\u05E0\u05E1", 56, 3),
    new Among("\u05E2\u05E1", 53, 1),
    new Among("\u05D9\u05E2\u05E1", 59, 2),
    new Among("\u05E2\u05DC\u05E2\u05E1", 59, 1),
    new Among("\u05E2\u05E8\u05E1", 53, 1),
    new Among("\u05E2\u05E0\u05E2\u05E8\u05E1", 62, 1),
    new Among("\u05E2", -1, 1),
    new Among("\u05D8\u05E2", 64, 4),
    new Among("\u05E1\u05D8\u05E2", 65, 1),
    new Among("\u05E2\u05D8\u05E2", 65, 1),
    new Among("\u05D9\u05E2", 64, -1),
    new Among("\u05E2\u05DC\u05E2", 64, 1),
    new Among("\u05E2\u05E0\u05E2", 64, 3),
    new Among("\u05D8\u05E2\u05E0\u05E2", 70, 4),
    new Among("\u05E2\u05E8", -1, 1),
    new Among("\u05D8\u05E2\u05E8", 72, 4),
    new Among("\u05E1\u05D8\u05E2\u05E8", 73, 1),
    new Among("\u05E2\u05D8\u05E2\u05E8", 73, 1),
    new Among("\u05E2\u05E0\u05E2\u05E8", 72, 3),
    new Among("\u05D8\u05E2\u05E0\u05E2\u05E8", 76, 4),
    new Among("\u05D5\u05EA", -1, 32)
  };

  private static final Among[] a_5 = {
    new Among("\u05D5\u05E0\u05D2", -1, 1),
    new Among("\u05E9\u05D0\u05E4\u05D8", -1, 1),
    new Among("\u05D4\u05F2\u05D8", -1, 1),
    new Among("\u05E7\u05F2\u05D8", -1, 1),
    new Among("\u05D9\u05E7\u05F2\u05D8", 3, 1),
    new Among("\u05DC", -1, 2)
  };

  private static final Among[] a_6 = {
    new Among("\u05D9\u05D2", -1, 1),
    new Among("\u05D9\u05E7", -1, 1),
    new Among("\u05D3\u05D9\u05E7", 1, 1),
    new Among("\u05E0\u05D3\u05D9\u05E7", 2, 1),
    new Among("\u05E2\u05E0\u05D3\u05D9\u05E7", 3, 1),
    new Among("\u05D1\u05DC\u05D9\u05E7", 1, -1),
    new Among("\u05D2\u05DC\u05D9\u05E7", 1, -1),
    new Among("\u05E0\u05D9\u05E7", 1, 1),
    new Among("\u05D9\u05E9", -1, 1)
  };

  private static final char[] g_niked = {255, 155, 6};

  private static final char[] g_vowel = {33, 2, 4, 0, 6};

  private static final char[] g_consonant = {239, 254, 253, 131};

  private int I_p1;

  private boolean r_prelude() {
    int among_var;
    int v_1 = cursor;
    lab0:
    {
      while (true) {
        int v_2 = cursor;
        lab1:
        {
          golab2:
          while (true) {
            int v_3 = cursor;
            lab3:
            {
              bra = cursor;
              among_var = find_among(a_0);
              if (among_var == 0) {
                break lab3;
              }
              ket = cursor;
              switch (among_var) {
                case 1:
                  {
                    int v_4 = cursor;
                    lab4:
                    {
                      if (!(eq_s("\u05BC"))) {
                        break lab4;
                      }
                      break lab3;
                    }
                    cursor = v_4;
                  }
                  slice_from("\u05F0");
                  break;
                case 2:
                  {
                    int v_5 = cursor;
                    lab5:
                    {
                      if (!(eq_s("\u05B4"))) {
                        break lab5;
                      }
                      break lab3;
                    }
                    cursor = v_5;
                  }
                  slice_from("\u05F1");
                  break;
                case 3:
                  {
                    int v_6 = cursor;
                    lab6:
                    {
                      if (!(eq_s("\u05B4"))) {
                        break lab6;
                      }
                      break lab3;
                    }
                    cursor = v_6;
                  }
                  slice_from("\u05F2");
                  break;
                case 4:
                  slice_from("\u05DB");
                  break;
                case 5:
                  slice_from("\u05DE");
                  break;
                case 6:
                  slice_from("\u05E0");
                  break;
                case 7:
                  slice_from("\u05E4");
                  break;
                case 8:
                  slice_from("\u05E6");
                  break;
              }
              cursor = v_3;
              break golab2;
            }
            cursor = v_3;
            if (cursor >= limit) {
              break lab1;
            }
            cursor++;
          }
          continue;
        }
        cursor = v_2;
        break;
      }
    }
    cursor = v_1;
    int v_7 = cursor;
    lab7:
    {
      while (true) {
        int v_8 = cursor;
        lab8:
        {
          golab9:
          while (true) {
            int v_9 = cursor;
            lab10:
            {
              bra = cursor;
              if (!(in_grouping(g_niked, 1456, 1474))) {
                break lab10;
              }
              ket = cursor;
              slice_del();
              cursor = v_9;
              break golab9;
            }
            cursor = v_9;
            if (cursor >= limit) {
              break lab8;
            }
            cursor++;
          }
          continue;
        }
        cursor = v_8;
        break;
      }
    }
    cursor = v_7;
    return true;
  }

  private boolean r_mark_regions() {
    int I_x;
    I_p1 = limit;
    int v_1 = cursor;
    lab0:
    {
      bra = cursor;
      if (!(eq_s("\u05D2\u05E2"))) {
        cursor = v_1;
        break lab0;
      }
      ket = cursor;
      {
        int v_2 = cursor;
        lab1:
        {
          lab2:
          {
            int v_3 = cursor;
            lab3:
            {
              if (!(eq_s("\u05DC\u05D8"))) {
                break lab3;
              }
              break lab2;
            }
            cursor = v_3;
            lab4:
            {
              if (!(eq_s("\u05D1\u05E0"))) {
                break lab4;
              }
              break lab2;
            }
            cursor = v_3;
            if (cursor < limit) {
              break lab1;
            }
          }
          cursor = v_1;
          break lab0;
        }
        cursor = v_2;
      }
      slice_from("GE");
    }
    int v_4 = cursor;
    lab5:
    {
      if (find_among(a_1) == 0) {
        cursor = v_4;
        break lab5;
      }
      lab6:
      {
        int v_5 = cursor;
        lab7:
        {
          int v_6 = cursor;
          lab8:
          {
            int v_7 = cursor;
            lab9:
            {
              if (!(eq_s("\u05E6\u05D5\u05D2\u05E0"))) {
                break lab9;
              }
              break lab8;
            }
            cursor = v_7;
            lab10:
            {
              if (!(eq_s("\u05E6\u05D5\u05E7\u05D8"))) {
                break lab10;
              }
              break lab8;
            }
            cursor = v_7;
            if (!(eq_s("\u05E6\u05D5\u05E7\u05E0"))) {
              break lab7;
            }
          }
          if (cursor < limit) {
            break lab7;
          }
          cursor = v_6;
          break lab6;
        }
        cursor = v_5;
        lab11:
        {
          int v_8 = cursor;
          if (!(eq_s("\u05D2\u05E2\u05D1\u05E0"))) {
            break lab11;
          }
          cursor = v_8;
          break lab6;
        }
        cursor = v_5;
        lab12:
        {
          bra = cursor;
          if (!(eq_s("\u05D2\u05E2"))) {
            break lab12;
          }
          ket = cursor;
          slice_from("GE");
          break lab6;
        }
        cursor = v_5;
        bra = cursor;
        if (!(eq_s("\u05E6\u05D5"))) {
          cursor = v_4;
          break lab5;
        }
        ket = cursor;
        slice_from("TSU");
      }
    }
    int v_9 = cursor;
    {
      int c = cursor + 3;
      if (c > limit) {
        return false;
      }
      cursor = c;
    }
    I_x = cursor;
    cursor = v_9;
    int v_10 = cursor;
    lab13:
    {
      if (find_among(a_2) == 0) {
        cursor = v_10;
        break lab13;
      }
    }
    {
      int v_11 = cursor;
      lab14:
      {
        if (!(in_grouping(g_consonant, 1489, 1520))) {
          break lab14;
        }
        if (!(in_grouping(g_consonant, 1489, 1520))) {
          break lab14;
        }
        if (!(in_grouping(g_consonant, 1489, 1520))) {
          break lab14;
        }
        I_p1 = cursor;
        return false;
      }
      cursor = v_11;
    }
    if (!go_out_grouping(g_vowel, 1488, 1522)) {
      return false;
    }
    cursor++;
    if (!go_in_grouping(g_vowel, 1488, 1522)) {
      return false;
    }
    I_p1 = cursor;
    lab15:
    {
      if (I_p1 >= I_x) {
        break lab15;
      }
      I_p1 = I_x;
    }
    return true;
  }

  private boolean r_R1() {
    return I_p1 <= cursor;
  }

  private boolean r_R1plus3() {
    return I_p1 <= (cursor + 3);
  }

  private boolean r_standard_suffix() {
    int among_var;
    int v_1 = limit - cursor;
    lab0:
    {
      ket = cursor;
      among_var = find_among_b(a_4);
      if (among_var == 0) {
        break lab0;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          if (!r_R1()) {
            break lab0;
          }
          slice_del();
          break;
        case 2:
          if (!r_R1()) {
            break lab0;
          }
          slice_from("\u05D9\u05E2");
          break;
        case 3:
          if (!r_R1()) {
            break lab0;
          }
          slice_del();
          ket = cursor;
          among_var = find_among_b(a_3);
          if (among_var == 0) {
            break lab0;
          }
          bra = cursor;
          switch (among_var) {
            case 1:
              slice_from("\u05D2\u05F2");
              break;
            case 2:
              slice_from("\u05E0\u05E2\u05DE");
              break;
            case 3:
              slice_from("\u05DE\u05F2\u05D3");
              break;
            case 4:
              slice_from("\u05D1\u05F2\u05D8");
              break;
            case 5:
              slice_from("\u05D1\u05F2\u05E1");
              break;
            case 6:
              slice_from("\u05F0\u05F2\u05D6");
              break;
            case 7:
              slice_from("\u05D8\u05E8\u05F2\u05D1");
              break;
            case 8:
              slice_from("\u05DC\u05F2\u05D8");
              break;
            case 9:
              slice_from("\u05E7\u05DC\u05F2\u05D1");
              break;
            case 10:
              slice_from("\u05E8\u05F2\u05D1");
              break;
            case 11:
              slice_from("\u05E8\u05F2\u05E1");
              break;
            case 12:
              slice_from("\u05E9\u05F0\u05F2\u05D2");
              break;
            case 13:
              slice_from("\u05E9\u05DE\u05F2\u05E1");
              break;
            case 14:
              slice_from("\u05E9\u05E0\u05F2\u05D3");
              break;
            case 15:
              slice_from("\u05E9\u05E8\u05F2\u05D1");
              break;
            case 16:
              slice_from("\u05D1\u05D9\u05E0\u05D3");
              break;
            case 17:
              slice_from("\u05F0\u05D9\u05D8\u05E9");
              break;
            case 18:
              slice_from("\u05D6\u05D9\u05E0\u05D2");
              break;
            case 19:
              slice_from("\u05D8\u05E8\u05D9\u05E0\u05E7");
              break;
            case 20:
              slice_from("\u05E6\u05F0\u05D9\u05E0\u05D2");
              break;
            case 21:
              slice_from("\u05E9\u05DC\u05D9\u05E0\u05D2");
              break;
            case 22:
              slice_from("\u05D1\u05F2\u05D2");
              break;
            case 23:
              slice_from("\u05D4\u05F2\u05D1");
              break;
            case 24:
              slice_from("\u05E4\u05D0\u05E8\u05DC\u05D9\u05E8");
              break;
            case 25:
              slice_from("\u05E9\u05D8\u05F2");
              break;
            case 26:
              slice_from("\u05E9\u05F0\u05E2\u05E8");
              break;
          }
          break;
        case 4:
          lab1:
          {
            int v_2 = limit - cursor;
            lab2:
            {
              if (!r_R1()) {
                break lab2;
              }
              slice_del();
              break lab1;
            }
            cursor = limit - v_2;
            slice_from("\u05D8");
          }
          ket = cursor;
          if (!(eq_s_b("\u05D1\u05E8\u05D0\u05DB"))) {
            break lab0;
          }
          int v_3 = limit - cursor;
          lab3:
          {
            if (!(eq_s_b("\u05D2\u05E2"))) {
              cursor = limit - v_3;
              break lab3;
            }
          }
          bra = cursor;
          slice_from("\u05D1\u05E8\u05E2\u05E0\u05D2");
          break;
        case 5:
          slice_from("\u05D2\u05F2");
          break;
        case 6:
          slice_from("\u05E0\u05E2\u05DE");
          break;
        case 7:
          slice_from("\u05E9\u05E8\u05F2\u05D1");
          break;
        case 8:
          slice_from("\u05DE\u05F2\u05D3");
          break;
        case 9:
          slice_from("\u05D1\u05F2\u05D8");
          break;
        case 10:
          slice_from("\u05D1\u05F2\u05E1");
          break;
        case 11:
          slice_from("\u05F0\u05F2\u05D6");
          break;
        case 12:
          slice_from("\u05D8\u05E8\u05F2\u05D1");
          break;
        case 13:
          slice_from("\u05DC\u05F2\u05D8");
          break;
        case 14:
          slice_from("\u05E7\u05DC\u05F2\u05D1");
          break;
        case 15:
          slice_from("\u05E8\u05F2\u05D1");
          break;
        case 16:
          slice_from("\u05E8\u05F2\u05E1");
          break;
        case 17:
          slice_from("\u05E9\u05F0\u05F2\u05D2");
          break;
        case 18:
          slice_from("\u05E9\u05DE\u05F2\u05E1");
          break;
        case 19:
          slice_from("\u05E9\u05E0\u05F2\u05D3");
          break;
        case 20:
          slice_from("\u05D1\u05D9\u05E0\u05D3");
          break;
        case 21:
          slice_from("\u05F0\u05D9\u05D8\u05E9");
          break;
        case 22:
          slice_from("\u05D6\u05D9\u05E0\u05D2");
          break;
        case 23:
          slice_from("\u05D8\u05E8\u05D9\u05E0\u05E7");
          break;
        case 24:
          slice_from("\u05E6\u05F0\u05D9\u05E0\u05D2");
          break;
        case 25:
          slice_from("\u05E9\u05DC\u05D9\u05E0\u05D2");
          break;
        case 26:
          slice_from("\u05D1\u05F2\u05D2");
          break;
        case 27:
          slice_from("\u05D4\u05F2\u05D1");
          break;
        case 28:
          slice_from("\u05E4\u05D0\u05E8\u05DC\u05D9\u05E8");
          break;
        case 29:
          slice_from("\u05E9\u05D8\u05F2");
          break;
        case 30:
          slice_from("\u05E9\u05F0\u05E2\u05E8");
          break;
        case 31:
          slice_from("\u05D1\u05E8\u05E2\u05E0\u05D2");
          break;
        case 32:
          if (!r_R1()) {
            break lab0;
          }
          slice_from("\u05D4");
          break;
        case 33:
          lab4:
          {
            int v_4 = limit - cursor;
            lab5:
            {
              lab6:
              {
                int v_5 = limit - cursor;
                lab7:
                {
                  if (!(eq_s_b("\u05D2"))) {
                    break lab7;
                  }
                  break lab6;
                }
                cursor = limit - v_5;
                if (!(eq_s_b("\u05E9"))) {
                  break lab5;
                }
              }
              int v_6 = limit - cursor;
              lab8:
              {
                if (!r_R1plus3()) {
                  cursor = limit - v_6;
                  break lab8;
                }
                slice_from("\u05D9\u05E1");
              }
              break lab4;
            }
            cursor = limit - v_4;
            if (!r_R1()) {
              break lab0;
            }
            slice_del();
          }
          break;
      }
    }
    cursor = limit - v_1;
    int v_7 = limit - cursor;
    lab9:
    {
      ket = cursor;
      among_var = find_among_b(a_5);
      if (among_var == 0) {
        break lab9;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          if (!r_R1()) {
            break lab9;
          }
          slice_del();
          break;
        case 2:
          if (!r_R1()) {
            break lab9;
          }
          if (!(in_grouping_b(g_consonant, 1489, 1520))) {
            break lab9;
          }
          slice_del();
          break;
      }
    }
    cursor = limit - v_7;
    int v_8 = limit - cursor;
    lab10:
    {
      ket = cursor;
      among_var = find_among_b(a_6);
      if (among_var == 0) {
        break lab10;
      }
      bra = cursor;
      switch (among_var) {
        case 1:
          if (!r_R1()) {
            break lab10;
          }
          slice_del();
          break;
      }
    }
    cursor = limit - v_8;
    int v_9 = limit - cursor;
    lab11:
    {
      while (true) {
        int v_10 = limit - cursor;
        lab12:
        {
          golab13:
          while (true) {
            int v_11 = limit - cursor;
            lab14:
            {
              ket = cursor;
              lab15:
              {
                int v_12 = limit - cursor;
                lab16:
                {
                  if (!(eq_s_b("GE"))) {
                    break lab16;
                  }
                  break lab15;
                }
                cursor = limit - v_12;
                if (!(eq_s_b("TSU"))) {
                  break lab14;
                }
              }
              bra = cursor;
              slice_del();
              cursor = limit - v_11;
              break golab13;
            }
            cursor = limit - v_11;
            if (cursor <= limit_backward) {
              break lab12;
            }
            cursor--;
          }
          continue;
        }
        cursor = limit - v_10;
        break;
      }
    }
    cursor = limit - v_9;
    return true;
  }

  @Override
  public boolean stem() {
    r_prelude();
    int v_1 = cursor;
    r_mark_regions();
    cursor = v_1;
    limit_backward = cursor;
    cursor = limit;
    r_standard_suffix();
    cursor = limit_backward;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof YiddishStemmer;
  }

  @Override
  public int hashCode() {
    return YiddishStemmer.class.getName().hashCode();
  }
}
