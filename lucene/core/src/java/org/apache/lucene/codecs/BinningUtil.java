package org.apache.lucene.codecs;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;

public final class BinningUtil {

  private BinningUtil() {}

  public static boolean isBinningEnabled(SegmentWriteState state) {
    for (FieldInfo fi : state.fieldInfos) {
      if ("true".equalsIgnoreCase(fi.getAttribute("doBinning"))) {
        return true;
      }
    }
    return false;
  }

  public static String getBinningField(SegmentWriteState state) {
    for (FieldInfo fi : state.fieldInfos) {
      if ("true".equalsIgnoreCase(fi.getAttribute("doBinning"))) {
        return fi.name;
      }
    }
    return null;
  }

  public static int getBinCount(SegmentWriteState state, int maxDoc) {
    for (FieldInfo fi : state.fieldInfos) {
      if ("true".equalsIgnoreCase(fi.getAttribute("doBinning"))) {
        String attr = fi.getAttribute("bin.count");
        if (attr != null) {
          return Integer.parseInt(attr);
        }
        return Math.max(1, Integer.highestOneBit(maxDoc >>> 4));
      }
    }
    return -1;
  }
}
