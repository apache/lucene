/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.luke.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.ArrayUtil;

/**
 * Util for serializing objects as JSON and deserializing JSON as Map/List/String/Double/Boolean.
 *
 * <p>Serializes objects (with restrictions on their property types) to JSON using reflection.
 *
 * <ul>
 *   <li>Only public, non-static, non-transient properties are serialized.
 *   <li>Maps are treated as JSON maps; unlike other non-primitive classes, their properties are not
 *       inspected via reflection
 *   <li>Keys are ordered according to the underlying object: object properties are listed in their
 *       declared order, while Map key order is defined by the Map class's entrySet() iteration.
 *   <li>duplicate keys may be produced if two Map keys' toString() are the same.
 *   <li>Object property names are converted to JSON map keys using hyphens in place of camel-case
 * </ul>
 */
public final class JsonUtil {
  private JsonUtil() {}

  /**
   * Serialize with no extra white space.
   *
   * @param t the object to serialize
   * @return a String containing a JSON document representing the object t
   */
  public static String asJson(Object t) {
    return asJson(t, null);
  }

  /**
   * @param t the object to serialize
   * @param indent the initial level of indentation, in spaces, or null, in which do not write any
   *     newlines or indentation. Normally 0 or null; additional levels of indentation are fixed at
   *     2 spaces per level.
   * @return a String containing a JSON document representing the object t
   */
  public static String asJson(Object t, Integer indent) {
    StringBuilder buf = new StringBuilder();
    asJson(t, buf, indent);
    return buf.toString();
  }

  /**
   * Writes a JSON document representing the object into the buffer. If the indent parameter is
   * non-null, the result is prettyprinted with the given indentation offset.
   *
   * @param t the object to serialize
   * @param buf the buffer to write to
   * @param indent the level of indentation, in spaces, or null, in which do not write any newlines
   *     or indentation.
   */
  private static void asJson(Object t, StringBuilder buf, Integer indent) {
    if (t == null) {
      buf.append("null");
      return;
    }
    Class<?> cls = t.getClass();
    if (isStringLike(cls)) {
      buf.append('"').append(jsonEscape(t.toString())).append('"');
    } else if (isAtomic(t.getClass())) {
      buf.append(t);
    } else if (t instanceof List) {
      asJsonArray((List<?>) t, buf, indent);
    } else if (cls.isArray()) {
      asJsonArray(t, buf, indent);
    } else if (t instanceof Map) {
      asJsonMap((Map<?, ?>) t, buf, indent);
    } else {
      asJsonObject(t, buf, indent);
    }
  }

  private static void asJsonObject(Object t, StringBuilder buf, Integer level) {
    buf.append('{');
    Class<?> cls = t.getClass();
    List<Field> fields = new ArrayList<>();
    getAllDeclaredFields(cls, fields);
    Integer nextLevel = addIndent(level);
    HashSet<String> seen = new HashSet<>();
    for (Field field : fields) {
      // Ignore static fields
      if (seen.contains(field.getName())) {
        continue;
      }
      seen.add(field.getName());
      int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers)
          || Modifier.isPrivate(modifiers)
          || Modifier.isTransient(modifiers)
          || ((modifiers & 4096) != 0)) { // check for super.this (this$0)
        continue;
      }
      indent(buf, nextLevel);
      buf.append('"').append(camelToUnderscore(field.getName())).append("\":");
      asJson(getValue(t, field), buf, nextLevel);
      buf.append(',');
    }
    rewindComma(buf, level);
    buf.append('}');
  }

  private static void asJsonMap(Map<?, ?> m, StringBuilder buf, Integer level) {
    buf.append('{');
    Integer nextLevel = addIndent(level);
    for (Map.Entry<?, ?> e : m.entrySet()) {
      indent(buf, nextLevel);
      buf.append('"').append(camelToUnderscore(e.getKey().toString())).append("\":");
      asJson(e.getValue(), buf, nextLevel);
      buf.append(',');
    }
    rewindComma(buf, level);
    buf.append('}');
  }

  private static void asJsonArray(List<?> t, StringBuilder buf, Integer level) {
    buf.append('[');
    Integer nextLevel = addIndent(level);
    for (Object value : t) {
      indent(buf, nextLevel);
      asJson(value, buf, nextLevel);
      buf.append(',');
    }
    rewindComma(buf, level);
    buf.append(']');
  }

  private static void asJsonArray(Object t, StringBuilder buf, Integer level) {
    assert t.getClass().isArray();
    buf.append('[');
    int len = Array.getLength(t);
    Integer nextLevel = addIndent(level);
    for (int i = 0; i < len; i++) {
      Object value = Array.get(t, i);
      indent(buf, nextLevel);
      asJson(value, buf, nextLevel);
      buf.append(',');
    }
    rewindComma(buf, level);
    buf.append(']');
  }

  private static void rewindComma(StringBuilder buf, Integer level) {
    if (buf.charAt(buf.length() - 1) == ',') {
      buf.setLength(buf.length() - 1);
      indent(buf, level);
    }
  }

  @SuppressWarnings("unused")
  private static Object getValue(Object o, Field f) {
    try {
      return f.get(o);
    } catch (IllegalAccessException e) {
      // just ignore inaccessible fields
      return null;
    }
  }

  private static String camelToUnderscore(String s) {
    return camelToSeparator(s, "_");
  }

  private static String camelToSeparator(String s, String separator) {
    StringBuilder buf = new StringBuilder(s.length());
    boolean upperLast = Character.isUpperCase(s.charAt(0));
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      boolean isUpper = Character.isUpperCase(c);
      if (isUpper && upperLast == false) {
        buf.append(separator);
      }
      buf.append(Character.toLowerCase(c));
      upperLast = isUpper;
    }
    return buf.toString();
  }

  static String jsonEscape(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        default:
          if (c < 0x10) {
            sb.append("\\u000").append(Integer.toString(c, 16));
          } else if (c < 0x20) {
            sb.append("\\u00").append(Integer.toString(c, 16));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  private static void indent(StringBuilder buf, Integer level) {
    if (level != null) {
      buf.append('\n');
      buf.append(" ".repeat(Math.max(0, level)));
    }
  }

  private static Integer addIndent(Integer level) {
    if (level == null) {
      return null;
    } else {
      return level + 2;
    }
  }

  private static boolean isStringLike(Class<?> cls) {
    return cls == String.class || cls.isEnum();
  }

  static boolean isAtomic(Class<?> cls) {
    return cls.isPrimitive() || Number.class.isAssignableFrom(cls) || cls == Boolean.class;
  }

  // get declared fields of the given class and its ancestors
  private static void getAllDeclaredFields(Class<?> cls, List<Field> fields) {
    fields.addAll(List.of(cls.getDeclaredFields()));
    Class<?> supra = cls.getSuperclass();
    if (supra != Object.class) {
      getAllDeclaredFields(supra, fields);
    }
  }

  public static Object parse(InputStream json) throws IOException, ParseException {
    return new Parser(json).parse();
  }

  public static Object parse(String s) throws IOException, ParseException {
    return parse(new ByteArrayInputStream(s.getBytes(UTF_8)));
  }

  /**
   * Somewhat functional JSON parser. Does not support Unicode escapes (\\u####). Performs no
   * buffering; probably should pass it a BufferedInputStream. Allows trailing commas in lists.
   * Repeated calls to parse() will attempt to read a sequence of objects from the stream.
   */
  private static class Parser {
    private static final byte[] ALSE = new byte[] {'a', 'l', 's', 'e'};
    private static final byte[] RUE = new byte[] {'r', 'u', 'e'};
    private static final byte[] ULL = new byte[] {'u', 'l', 'l'};

    private final PushbackInputStream in;

    private int offset;
    private byte[] buf = new byte[1024];

    Parser(InputStream in) {
      this.in = new PushbackInputStream(in);
    }

    Object parse() throws IOException, ParseException {
      int b;
      if ((b = in.read()) != -1) {
        ++offset;
        return parse(b);
      }
      throw new ParseException("premature EOF", 0);
    }

    Object parse(int b) throws IOException, ParseException {
      do {
        switch (b) {
          case '{':
            return parseMap();
          case '[':
            return parseList();
          case '"':
            return parseString();
          case 'f':
            parseToken(ALSE);
            return false;
          case 't':
            parseToken(RUE);
            return true;
          case 'n':
            parseToken(ULL);
            return null;
          case '.':
          case '-':
          case '+':
          case '0':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            return parseNumber((byte) b);
          case ' ':
          case '\b':
          case '\f':
          case '\n':
          case '\r':
          case '\t':
            break;
          default:
            throw new ParseException("unexpected byte " + b, offset);
        }
      } while ((b = in.read()) != -1);
      throw new ParseException("premature EOF", offset);
    }

    private Map<String, Object> parseMap() throws IOException, ParseException {
      HashMap<String, Object> map = new HashMap<>();
      boolean expectComma = false;
      int b;
      while ((b = in.read()) != -1) {
        ++offset;
        switch (b) {
          case '}':
            return map;
          case ',':
            if (expectComma) {
              expectComma = false;
            } else {
              throw new ParseException("unexpected comma", offset);
            }
            break;
          case '"':
            String key = parseString();
            parseColon();
            map.put(key, parse());
            expectComma = true;
            break;
          default:
            if (Character.isWhitespace(b)) {
              break;
            }
            throw new ParseException("unexpected byte " + b, offset);
        }
      }
      throw new ParseException("premature EOF", offset);
    }

    private void parseColon() throws IOException, ParseException {
      int b;
      while ((b = in.read()) != -1) {
        ++offset;
        if (b == ':') {
          return;
        } else if (Character.isWhitespace(b) == false) {
          throw new ParseException("unexpected byte " + b, offset);
        }
      }
    }

    private List<Object> parseList() throws IOException, ParseException {
      List<Object> list = new ArrayList<>();
      boolean expectComma = false;
      int b;
      while ((b = in.read()) != -1) {
        ++offset;
        switch (b) {
          case ']':
            return list;
          case ',':
            if (expectComma) {
              expectComma = false;
            } else {
              throw new ParseException("unexpected comma", offset);
            }
            break;
          default:
            if (expectComma) {
              if (Character.isWhitespace(b) == false) {
                throw new ParseException("unexpected byte " + b, offset);
              }
            } else {
              list.add(parse(b));
              expectComma = true;
            }
            break;
        }
      }
      throw new ParseException("premature EOF", offset);
    }

    private Double parseNumber(byte initial) throws IOException, ParseException {
      buf[0] = initial;
      int b;
      int len = 1;
      OUTER:
      while ((b = in.read()) != -1) {
        ++offset;
        switch (b) {
          case '.':
          case 'e':
          case 'E':
          case '-':
          case '+':
          case '0':
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            buf[len++] = (byte) b;
            break;
          default:
            break OUTER;
        }
      }
      try {
        Double result = Double.parseDouble(new String(buf, 0, len));
        if (b != -1) {
          // we had to look ahead to find the termination of the number; push back the last byte
          in.unread((byte) b);
        }
        return result;
      } catch (NumberFormatException nfe) {
        throw new ParseException(
            "malformed number: '" + new String(buf, 0, len) + "' " + nfe.getMessage(), offset);
      }
    }

    private String parseString() throws IOException, ParseException {
      int n = 0;
      int b;
      while ((b = in.read()) != -1) {
        if (n == buf.length) {
          buf = ArrayUtil.grow(buf);
        }
        switch (b) {
          case '"':
            return new String(buf, 0, n, UTF_8);

          case '\\':
            buf[n++] = parseEscaped(offset + n);
            break;

          default:
            buf[n++] = (byte) b;
            break;
        }
      }
      throw new ParseException("premature EOF", offset);
    }

    private byte parseEscaped(int pos) throws IOException, ParseException {
      int b = in.read();
      if (b == -1) {
        throw new ParseException("premature EOF", offset);
      }
      switch (b) {
        case 'b':
          return '\b';
        case 'f':
          return '\f';
        case 'n':
          return '\n';
        case 'r':
          return '\r';
        case 't':
          return '\t';
        case '\\':
        case '"':
          return (byte) b;
        default:
          throw new ParseException("unsupported character escape: '" + b + "'", pos);
      }
    }

    private void parseToken(byte[] token) throws IOException, ParseException {
      if (in.read(buf, 0, token.length) == token.length
          && Arrays.equals(buf, 0, token.length, token, 0, token.length)) {
        offset += token.length;
      } else {
        throw new ParseException("unknown token", offset);
      }
    }
  }
}
