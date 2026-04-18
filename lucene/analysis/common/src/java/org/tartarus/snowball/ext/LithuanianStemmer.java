// Generated from lithuanian.sbl by Snowball 3.0.0 - https://snowballstem.org/

package org.tartarus.snowball.ext;

import org.tartarus.snowball.Among;

/**
 * This class implements the stemming algorithm defined by a snowball script.
 *
 * <p>Generated from lithuanian.sbl by Snowball 3.0.0 - https://snowballstem.org/
 */
@SuppressWarnings("unused")
public class LithuanianStemmer extends org.tartarus.snowball.SnowballStemmer {

  private static final long serialVersionUID = 1L;

  private static final Among[] a_0 = {
    new Among("a", -1, -1),
    new Among("ia", 0, -1),
    new Among("osna", 0, -1),
    new Among("iosna", 2, -1),
    new Among("uosna", 2, -1),
    new Among("iuosna", 4, -1),
    new Among("ysna", 0, -1),
    new Among("\u0117sna", 0, -1),
    new Among("e", -1, -1),
    new Among("ie", 8, -1),
    new Among("enie", 9, -1),
    new Among("oje", 8, -1),
    new Among("ioje", 11, -1),
    new Among("uje", 8, -1),
    new Among("iuje", 13, -1),
    new Among("yje", 8, -1),
    new Among("enyje", 15, -1),
    new Among("\u0117je", 8, -1),
    new Among("ame", 8, -1),
    new Among("iame", 18, -1),
    new Among("sime", 8, -1),
    new Among("ome", 8, -1),
    new Among("\u0117me", 8, -1),
    new Among("tum\u0117me", 22, -1),
    new Among("ose", 8, -1),
    new Among("iose", 24, -1),
    new Among("uose", 24, -1),
    new Among("iuose", 26, -1),
    new Among("yse", 8, -1),
    new Among("enyse", 28, -1),
    new Among("\u0117se", 8, -1),
    new Among("ate", 8, -1),
    new Among("iate", 31, -1),
    new Among("ite", 8, -1),
    new Among("kite", 33, -1),
    new Among("site", 33, -1),
    new Among("ote", 8, -1),
    new Among("tute", 8, -1),
    new Among("\u0117te", 8, -1),
    new Among("tum\u0117te", 38, -1),
    new Among("i", -1, -1),
    new Among("ai", 40, -1),
    new Among("iai", 41, -1),
    new Among("ei", 40, -1),
    new Among("tumei", 43, -1),
    new Among("ki", 40, -1),
    new Among("imi", 40, -1),
    new Among("umi", 40, -1),
    new Among("iumi", 47, -1),
    new Among("si", 40, -1),
    new Among("asi", 49, -1),
    new Among("iasi", 50, -1),
    new Among("esi", 49, -1),
    new Among("iesi", 52, -1),
    new Among("siesi", 53, -1),
    new Among("isi", 49, -1),
    new Among("aisi", 55, -1),
    new Among("eisi", 55, -1),
    new Among("tumeisi", 57, -1),
    new Among("uisi", 55, -1),
    new Among("osi", 49, -1),
    new Among("\u0117josi", 60, -1),
    new Among("uosi", 60, -1),
    new Among("iuosi", 62, -1),
    new Among("siuosi", 63, -1),
    new Among("usi", 49, -1),
    new Among("ausi", 65, -1),
    new Among("\u010Diausi", 66, -1),
    new Among("\u0105si", 49, -1),
    new Among("\u0117si", 49, -1),
    new Among("\u0173si", 49, -1),
    new Among("t\u0173si", 70, -1),
    new Among("ti", 40, -1),
    new Among("enti", 72, -1),
    new Among("inti", 72, -1),
    new Among("oti", 72, -1),
    new Among("ioti", 75, -1),
    new Among("uoti", 75, -1),
    new Among("iuoti", 77, -1),
    new Among("auti", 72, -1),
    new Among("iauti", 79, -1),
    new Among("yti", 72, -1),
    new Among("\u0117ti", 72, -1),
    new Among("tel\u0117ti", 82, -1),
    new Among("in\u0117ti", 82, -1),
    new Among("ter\u0117ti", 82, -1),
    new Among("ui", 40, -1),
    new Among("iui", 86, -1),
    new Among("eniui", 87, -1),
    new Among("oj", -1, -1),
    new Among("\u0117j", -1, -1),
    new Among("k", -1, -1),
    new Among("am", -1, -1),
    new Among("iam", 92, -1),
    new Among("iem", -1, -1),
    new Among("im", -1, -1),
    new Among("sim", 95, -1),
    new Among("om", -1, -1),
    new Among("tum", -1, -1),
    new Among("\u0117m", -1, -1),
    new Among("tum\u0117m", 99, -1),
    new Among("an", -1, -1),
    new Among("on", -1, -1),
    new Among("ion", 102, -1),
    new Among("un", -1, -1),
    new Among("iun", 104, -1),
    new Among("\u0117n", -1, -1),
    new Among("o", -1, -1),
    new Among("io", 107, -1),
    new Among("enio", 108, -1),
    new Among("\u0117jo", 107, -1),
    new Among("uo", 107, -1),
    new Among("s", -1, -1),
    new Among("as", 112, -1),
    new Among("ias", 113, -1),
    new Among("es", 112, -1),
    new Among("ies", 115, -1),
    new Among("is", 112, -1),
    new Among("ais", 117, -1),
    new Among("iais", 118, -1),
    new Among("tumeis", 117, -1),
    new Among("imis", 117, -1),
    new Among("enimis", 121, -1),
    new Among("omis", 117, -1),
    new Among("iomis", 123, -1),
    new Among("umis", 117, -1),
    new Among("\u0117mis", 117, -1),
    new Among("enis", 117, -1),
    new Among("asis", 117, -1),
    new Among("ysis", 117, -1),
    new Among("ams", 112, -1),
    new Among("iams", 130, -1),
    new Among("iems", 112, -1),
    new Among("ims", 112, -1),
    new Among("enims", 133, -1),
    new Among("oms", 112, -1),
    new Among("ioms", 135, -1),
    new Among("ums", 112, -1),
    new Among("\u0117ms", 112, -1),
    new Among("ens", 112, -1),
    new Among("os", 112, -1),
    new Among("ios", 140, -1),
    new Among("uos", 140, -1),
    new Among("iuos", 142, -1),
    new Among("us", 112, -1),
    new Among("aus", 144, -1),
    new Among("iaus", 145, -1),
    new Among("ius", 144, -1),
    new Among("ys", 112, -1),
    new Among("enys", 148, -1),
    new Among("\u0105s", 112, -1),
    new Among("i\u0105s", 150, -1),
    new Among("\u0117s", 112, -1),
    new Among("am\u0117s", 152, -1),
    new Among("iam\u0117s", 153, -1),
    new Among("im\u0117s", 152, -1),
    new Among("kim\u0117s", 155, -1),
    new Among("sim\u0117s", 155, -1),
    new Among("om\u0117s", 152, -1),
    new Among("\u0117m\u0117s", 152, -1),
    new Among("tum\u0117m\u0117s", 159, -1),
    new Among("at\u0117s", 152, -1),
    new Among("iat\u0117s", 161, -1),
    new Among("sit\u0117s", 152, -1),
    new Among("ot\u0117s", 152, -1),
    new Among("\u0117t\u0117s", 152, -1),
    new Among("tum\u0117t\u0117s", 165, -1),
    new Among("\u012Fs", 112, -1),
    new Among("\u016Bs", 112, -1),
    new Among("t\u0173s", 112, -1),
    new Among("at", -1, -1),
    new Among("iat", 170, -1),
    new Among("it", -1, -1),
    new Among("sit", 172, -1),
    new Among("ot", -1, -1),
    new Among("\u0117t", -1, -1),
    new Among("tum\u0117t", 175, -1),
    new Among("u", -1, -1),
    new Among("au", 177, -1),
    new Among("iau", 178, -1),
    new Among("\u010Diau", 179, -1),
    new Among("iu", 177, -1),
    new Among("eniu", 181, -1),
    new Among("siu", 181, -1),
    new Among("y", -1, -1),
    new Among("\u0105", -1, -1),
    new Among("i\u0105", 185, -1),
    new Among("\u0117", -1, -1),
    new Among("\u0119", -1, -1),
    new Among("\u012F", -1, -1),
    new Among("en\u012F", 189, -1),
    new Among("\u0173", -1, -1),
    new Among("i\u0173", 191, -1)
  };

  private static final Among[] a_1 = {
    new Among("ing", -1, -1),
    new Among("aj", -1, -1),
    new Among("iaj", 1, -1),
    new Among("iej", -1, -1),
    new Among("oj", -1, -1),
    new Among("ioj", 4, -1),
    new Among("uoj", 4, -1),
    new Among("iuoj", 6, -1),
    new Among("auj", -1, -1),
    new Among("\u0105j", -1, -1),
    new Among("i\u0105j", 9, -1),
    new Among("\u0117j", -1, -1),
    new Among("\u0173j", -1, -1),
    new Among("i\u0173j", 12, -1),
    new Among("ok", -1, -1),
    new Among("iok", 14, -1),
    new Among("iuk", -1, -1),
    new Among("uliuk", 16, -1),
    new Among("u\u010Diuk", 16, -1),
    new Among("i\u0161k", -1, -1),
    new Among("iul", -1, -1),
    new Among("yl", -1, -1),
    new Among("\u0117l", -1, -1),
    new Among("am", -1, -1),
    new Among("dam", 23, -1),
    new Among("jam", 23, -1),
    new Among("zgan", -1, -1),
    new Among("ain", -1, -1),
    new Among("esn", -1, -1),
    new Among("op", -1, -1),
    new Among("iop", 29, -1),
    new Among("ias", -1, -1),
    new Among("ies", -1, -1),
    new Among("ais", -1, -1),
    new Among("iais", 33, -1),
    new Among("os", -1, -1),
    new Among("ios", 35, -1),
    new Among("uos", 35, -1),
    new Among("iuos", 37, -1),
    new Among("aus", -1, -1),
    new Among("iaus", 39, -1),
    new Among("\u0105s", -1, -1),
    new Among("i\u0105s", 41, -1),
    new Among("\u0119s", -1, -1),
    new Among("ut\u0117ait", -1, -1),
    new Among("ant", -1, -1),
    new Among("iant", 45, -1),
    new Among("siant", 46, -1),
    new Among("int", -1, -1),
    new Among("ot", -1, -1),
    new Among("uot", 49, -1),
    new Among("iuot", 50, -1),
    new Among("yt", -1, -1),
    new Among("\u0117t", -1, -1),
    new Among("yk\u0161t", -1, -1),
    new Among("iau", -1, -1),
    new Among("dav", -1, -1),
    new Among("sv", -1, -1),
    new Among("\u0161v", -1, -1),
    new Among("yk\u0161\u010D", -1, -1),
    new Among("\u0119", -1, -1),
    new Among("\u0117j\u0119", 60, -1)
  };

  private static final Among[] a_2 = {
    new Among("ojime", -1, 7),
    new Among("\u0117jime", -1, 3),
    new Among("avime", -1, 6),
    new Among("okate", -1, 8),
    new Among("aite", -1, 1),
    new Among("uote", -1, 2),
    new Among("asius", -1, 5),
    new Among("okat\u0117s", -1, 8),
    new Among("ait\u0117s", -1, 1),
    new Among("uot\u0117s", -1, 2),
    new Among("esiu", -1, 4)
  };

  private static final Among[] a_3 = {new Among("\u010D", -1, 1), new Among("d\u017E", -1, 2)};

  private static final char[] g_v = {
    17, 65, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 64, 1, 0, 64, 0, 0, 0, 0,
    0, 0, 0, 4, 4
  };

  private int I_p1;

  private boolean r_step1() {
    if (cursor < I_p1) {
      return false;
    }
    int v_1 = limit_backward;
    limit_backward = I_p1;
    ket = cursor;
    if (find_among_b(a_0) == 0) {
      limit_backward = v_1;
      return false;
    }
    bra = cursor;
    limit_backward = v_1;
    slice_del();
    return true;
  }

  private boolean r_step2() {
    while (true) {
      int v_1 = limit - cursor;
      lab0:
      {
        if (cursor < I_p1) {
          break lab0;
        }
        int v_2 = limit_backward;
        limit_backward = I_p1;
        ket = cursor;
        if (find_among_b(a_1) == 0) {
          limit_backward = v_2;
          break lab0;
        }
        bra = cursor;
        limit_backward = v_2;
        slice_del();
        continue;
      }
      cursor = limit - v_1;
      break;
    }
    return true;
  }

  private boolean r_fix_conflicts() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_2);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        slice_from("ait\u0117");
        break;
      case 2:
        slice_from("uot\u0117");
        break;
      case 3:
        slice_from("\u0117jimas");
        break;
      case 4:
        slice_from("esys");
        break;
      case 5:
        slice_from("asys");
        break;
      case 6:
        slice_from("avimas");
        break;
      case 7:
        slice_from("ojimas");
        break;
      case 8:
        slice_from("okat\u0117");
        break;
    }
    return true;
  }

  private boolean r_fix_chdz() {
    int among_var;
    ket = cursor;
    among_var = find_among_b(a_3);
    if (among_var == 0) {
      return false;
    }
    bra = cursor;
    switch (among_var) {
      case 1:
        slice_from("t");
        break;
      case 2:
        slice_from("d");
        break;
    }
    return true;
  }

  private boolean r_fix_gd() {
    ket = cursor;
    if (!(eq_s_b("gd"))) {
      return false;
    }
    bra = cursor;
    slice_from("g");
    return true;
  }

  @Override
  public boolean stem() {
    I_p1 = limit;
    int v_1 = cursor;
    lab0:
    {
      int v_2 = cursor;
      lab1:
      {
        if (!(eq_s("a"))) {
          cursor = v_2;
          break lab1;
        }
        if (length <= 6) {
          cursor = v_2;
          break lab1;
        }
      }
      if (!go_out_grouping(g_v, 97, 371)) {
        break lab0;
      }
      cursor++;
      if (!go_in_grouping(g_v, 97, 371)) {
        break lab0;
      }
      cursor++;
      I_p1 = cursor;
    }
    cursor = v_1;
    limit_backward = cursor;
    cursor = limit;
    int v_3 = limit - cursor;
    r_fix_conflicts();
    cursor = limit - v_3;
    int v_4 = limit - cursor;
    r_step1();
    cursor = limit - v_4;
    int v_5 = limit - cursor;
    r_fix_chdz();
    cursor = limit - v_5;
    int v_6 = limit - cursor;
    r_step2();
    cursor = limit - v_6;
    int v_7 = limit - cursor;
    r_fix_chdz();
    cursor = limit - v_7;
    int v_8 = limit - cursor;
    r_fix_gd();
    cursor = limit - v_8;
    cursor = limit_backward;
    return true;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof LithuanianStemmer;
  }

  @Override
  public int hashCode() {
    return LithuanianStemmer.class.getName().hashCode();
  }
}
