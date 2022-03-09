package org.apache.lucene.analysis.ja.util;

import org.apache.lucene.analysis.ja.dict.TokenInfoMorphAttributes;
import org.apache.lucene.analysis.morph.DictionaryEntryWriter;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TokenInfoDictionaryEntryWriter extends DictionaryEntryWriter {
  private static final int ID_LIMIT = 8192;

  TokenInfoDictionaryEntryWriter(int size) {
    super(size);
  }

  @Override
  public int putEntry(String[] entry) {
    short leftId = Short.parseShort(entry[1]);
    short rightId = Short.parseShort(entry[2]);
    short wordCost = Short.parseShort(entry[3]);

    StringBuilder sb = new StringBuilder();

    // build up the POS string
    for (int i = 4; i < 8; i++) {
      String part = entry[i];
      assert part.length() > 0;
      if (!"*".equals(part)) {
        if (sb.length() > 0) {
          sb.append('-');
        }
        sb.append(part);
      }
    }

    String posData = sb.toString();
    if (posData.isEmpty()) {
      throw new IllegalArgumentException("POS fields are empty");
    }
    sb.setLength(0);
    sb.append(CSVUtil.quoteEscape(posData));
    sb.append(',');
    if (!"*".equals(entry[8])) {
      sb.append(CSVUtil.quoteEscape(entry[8]));
    }
    sb.append(',');
    if (!"*".equals(entry[9])) {
      sb.append(CSVUtil.quoteEscape(entry[9]));
    }
    String fullPOSData = sb.toString();

    String baseForm = entry[10];
    String reading = entry[11];
    String pronunciation = entry[12];

    // extend buffer if necessary
    int left = buffer.remaining();
    // worst case: two short, 3 bytes, and features (all as utf-16)
    int worstCase = 4 + 3 + 2 * (baseForm.length() + reading.length() + pronunciation.length());
    if (worstCase > left) {
      ByteBuffer newBuffer =
        ByteBuffer.allocateDirect(ArrayUtil.oversize(buffer.limit() + worstCase - left, 1));
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }

    int flags = 0;
    if (baseForm.isEmpty()) {
      throw new IllegalArgumentException("base form is empty");
    }
    if (!("*".equals(baseForm) || baseForm.equals(entry[0]))) {
      flags |= TokenInfoMorphAttributes.HAS_BASEFORM;
    }
    if (!reading.equals(toKatakana(entry[0]))) {
      flags |= TokenInfoMorphAttributes.HAS_READING;
    }
    if (!pronunciation.equals(reading)) {
      flags |= TokenInfoMorphAttributes.HAS_PRONUNCIATION;
    }

    if (leftId != rightId) {
      throw new IllegalArgumentException("rightId != leftId: " + rightId + " " + leftId);
    }
    if (leftId >= ID_LIMIT) {
      throw new IllegalArgumentException("leftId >= " + ID_LIMIT + ": " + leftId);
    }
    // add pos mapping
    int toFill = 1 + leftId - posDict.size();
    for (int i = 0; i < toFill; i++) {
      posDict.add(null);
    }

    String existing = posDict.get(leftId);
    if (existing != null && existing.equals(fullPOSData) == false) {
      // TODO: test me
      throw new IllegalArgumentException("Multiple entries found for leftID=" + leftId);
    }
    posDict.set(leftId, fullPOSData);

    buffer.putShort((short) (leftId << 3 | flags));
    buffer.putShort(wordCost);

    if ((flags & TokenInfoMorphAttributes.HAS_BASEFORM) != 0) {
      if (baseForm.length() >= 16) {
        throw new IllegalArgumentException("Length of base form " + baseForm + " is >= 16");
      }
      int shared = sharedPrefix(entry[0], baseForm);
      int suffix = baseForm.length() - shared;
      buffer.put((byte) (shared << 4 | suffix));
      for (int i = shared; i < baseForm.length(); i++) {
        buffer.putChar(baseForm.charAt(i));
      }
    }

    if ((flags & TokenInfoMorphAttributes.HAS_READING) != 0) {
      if (isKatakana(reading)) {
        buffer.put((byte) (reading.length() << 1 | 1));
        writeKatakana(reading, buffer);
      } else {
        buffer.put((byte) (reading.length() << 1));
        for (int i = 0; i < reading.length(); i++) {
          buffer.putChar(reading.charAt(i));
        }
      }
    }

    if ((flags & TokenInfoMorphAttributes.HAS_PRONUNCIATION) != 0) {
      // we can save 150KB here, but it makes the reader a little complicated.
      // int shared = sharedPrefix(reading, pronunciation);
      // buffer.put((byte) shared);
      // pronunciation = pronunciation.substring(shared);
      if (isKatakana(pronunciation)) {
        buffer.put((byte) (pronunciation.length() << 1 | 1));
        writeKatakana(pronunciation, buffer);
      } else {
        buffer.put((byte) (pronunciation.length() << 1));
        for (int i = 0; i < pronunciation.length(); i++) {
          buffer.putChar(pronunciation.charAt(i));
        }
      }
    }

    return buffer.position();
  }

  private boolean isKatakana(String s) {
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch < 0x30A0 || ch > 0x30FF) {
        return false;
      }
    }
    return true;
  }

  private void writeKatakana(String s, ByteBuffer buffer) {
    for (int i = 0; i < s.length(); i++) {
      buffer.put((byte) (s.charAt(i) - 0x30A0));
    }
  }

  private String toKatakana(String s) {
    char[] text = new char[s.length()];
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch > 0x3040 && ch < 0x3097) {
        text[i] = (char) (ch + 0x60);
      } else {
        text[i] = ch;
      }
    }
    return new String(text);
  }

  private static int sharedPrefix(String left, String right) {
    int len = left.length() < right.length() ? left.length() : right.length();
    for (int i = 0; i < len; i++) if (left.charAt(i) != right.charAt(i)) return i;
    return len;
  }

  protected void writePosDict(OutputStream bos, DataOutput out) throws IOException {
    out.writeVInt(posDict.size());
    for (String s : posDict) {
      if (s == null) {
        out.writeByte((byte) 0);
        out.writeByte((byte) 0);
        out.writeByte((byte) 0);
      } else {
        String[] data = CSVUtil.parse(s);
        if (data.length != 3) {
          throw new IllegalArgumentException(
            "Malformed pos/inflection: " + s + "; expected 3 characters");
        }
        out.writeString(data[0]);
        out.writeString(data[1]);
        out.writeString(data[2]);
      }
    }
  }

}
