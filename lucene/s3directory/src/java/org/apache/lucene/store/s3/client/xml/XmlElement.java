/* XMLElement.java
 *
 * Revision: 1.4 $
 * Date: 2002/03/24 10:27:59 $
 * $Name: RELEASE_2_2_1 $
 *
 * This file is part of NanoXML 2 Lite.
 * Copyright (C) 2000-2002 Marc De Scheemaecker, All Rights Reserved.
 *
 * This software is provided 'as-is', without any express or implied warranty.
 * In no event will the authors be held liable for any damages arising from the
 * use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software in
 *     a product, an acknowledgment in the product documentation would be
 *     appreciated but is not required.
 *
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *
 *  3. This notice may not be removed or altered from any source distribution.
 *****************************************************************************/

// ALTERED greatly by Dave Moten May 2021

package org.apache.lucene.store.s3.client.xml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.store.s3.client.internal.util.Preconditions;

/*
 * The NanoXML 2 Lite licence blurb is included here. The class has been
 * completely butchered but the core xml parsing routines are thanks to
 * the NanoXML authors.
 *
 **/

/* XmlElement.java
 *
 * Revision: 1.4 $
 * Date: 2002/03/24 10:27:59 $
 * $Name: RELEASE_2_2_1 $
 *
 * This file is part of NanoXML 2 Lite.
 * Copyright (C) 2000-2002 Marc De Scheemaecker, All Rights Reserved.
 *
 * This software is provided 'as-is', without any express or implied warranty.
 * In no event will the authors be held liable for any damages arising from the
 * use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 *  1. The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software in
 *     a product, an acknowledgment in the product documentation would be
 *     appreciated but is not required.
 *
 *  2. Altered source versions must be plainly marked as such, and must not be
 *     misrepresented as being the original software.
 *
 *  3. This notice may not be removed or altered from any source distribution.
 *****************************************************************************/

public final class XmlElement {

  private List<XmlElement> children;
  private Map<String, String> attributes;
  private String name;

  /** The #PCDATA content of the object. null if no #PCDATA, can be empty string */
  private String content; // non-null

  private static final Map<String, char[]> ENTITIES = createEntities();

  /** The line number where the element starts. */
  private int lineNr;

  /**
   * <code>true</code> if the leading and trailing whitespace of #PCDATA sections have to be
   * ignored.
   */
  private final boolean ignoreLeadingAndTrailingWhitespace;

  /**
   * Character read too much. This character provides push-back functionality to the input reader
   * without having to use a PushbackReader. If there is no such character, this field is '\0'.
   */
  private char charReadTooMuch;

  /** The reader provided by the caller of the parse method. */
  private Reader reader;

  /** The current line number in the source content. */
  private int parserLineNr;

  private XmlElement(boolean ignoreLeadingAndTrailingWhitespace) {
    this.ignoreLeadingAndTrailingWhitespace = ignoreLeadingAndTrailingWhitespace;
    this.name = null;
    this.content = "";
    this.attributes = new HashMap<>();
    this.children = new ArrayList<>();
    this.lineNr = 0;
  }

  public void addChild(XmlElement child) {
    children.add(child);
  }

  public int countChildren() {
    return children.size();
  }

  public boolean hasChildren() {
    return !children.isEmpty();
  }

  public Set<String> attributeNames() {
    return attributes.keySet();
  }

  public List<XmlElement> children() {
    return children;
  }

  public List<XmlElement> childrenWithName(String name) {
    return children.stream().filter(x -> name.equals(x.name())).collect(Collectors.toList());
  }

  public XmlElement firstChild() {
    return children.get(0);
  }

  public XmlElement child(int index) {
    return children.get(index);
  }

  public XmlElement child(String... names) {
    XmlElement x = this;
    XmlElement y = null;
    for (String name : names) {
      for (XmlElement child : x.children) {
        if (child.name().equals(name)) {
          y = child;
        }
      }
      if (y == null) {
        throw new NoSuchElementException("child not found with name: " + name);
      } else {
        x = y;
      }
    }
    return y;
  }

  public String content(String... names) {
    return child(names).content();
  }

  /**
   * Returns the PCDATA content of the object. If there is no such content, <CODE>null</CODE> is
   * returned.
   */
  public String content() {
    return this.content;
  }

  /**
   * Returns the line nr in the source data on which the element is found. This method returns
   * <code>0</code> there is no associated source data.
   */
  public int lineNumber() {
    return this.lineNr;
  }

  /**
   * Returns an attribute of the element. If the attribute doesn't exist, <code>null</code> is
   * returned.
   *
   * @param name The name of the attribute.
   */
  public String attribute(String name) {
    return this.attribute(name, null);
  }

  /**
   * Returns an attribute of the element. If the attribute doesn't exist, <code>defaultValue</code>
   * is returned.
   *
   * @param name The name of the attribute.
   * @param defaultValue Key to use if the attribute is missing.
   */
  public String attribute(String name, String defaultValue) {
    Preconditions.checkNotNull(name);
    return this.attributes.getOrDefault(name, defaultValue);
  }

  /** Returns the name of the element. */
  public String name() {
    return this.name;
  }

  public static XmlElement parse(Reader reader) throws XmlParseException, IOException {
    return parse(reader, true);
  }

  public static XmlElement parse(Reader reader, boolean ignoreLeadingAndTrailingWhitespace)
      throws IOException, XmlParseException {
    Preconditions.checkNotNull(reader);
    XmlElement x = new XmlElement(ignoreLeadingAndTrailingWhitespace);
    x.parseFromReader(reader);
    return x;
  }

  private void parseFromReader(Reader reader) throws IOException, XmlParseException {
    Preconditions.checkNotNull(reader);
    this.name = null;
    this.content = "";
    this.attributes = new HashMap<>();
    this.children = new ArrayList<>();
    this.charReadTooMuch = '\0';
    this.reader = reader;
    this.parserLineNr = 1;

    for (; ; ) {
      char ch = this.scanWhitespace();

      if (ch != '<') {
        throw this.createUnexpectedInputException("<");
      }

      ch = this.readChar();

      if ((ch == '!') || (ch == '?')) {
        this.skipSpecialTag(0);
      } else {
        this.unreadChar(ch);
        this.scanElement(this);
        return;
      }
    }
  }

  public static XmlElement parse(String string) throws XmlParseException {
    return parse(string, true);
  }

  public static XmlElement parse(String string, boolean ignoreLeadingAndTrailingWhitespace)
      throws XmlParseException {
    Preconditions.checkNotNull(string);
    return parseUnchecked(new StringReader(string), ignoreLeadingAndTrailingWhitespace);
  }

  // VisibleForTesting
  static XmlElement parseUnchecked(Reader reader, boolean ignoreLeadingAndTrailingWhitespace)
      throws XmlParseException {
    try {
      return parse(reader, ignoreLeadingAndTrailingWhitespace);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private XmlElement createAnotherElement() {
    return new XmlElement(this.ignoreLeadingAndTrailingWhitespace);
  }

  @Override
  public String toString() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    writeUnchecked(writer);
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  // visible for testing
  void writeUnchecked(Writer writer) {
    try {
      this.write(writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public void write(Writer writer) throws IOException {
    Preconditions.checkNotNull(writer);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(content);
    //        if (this.name == null) {
    //            writeEncoded(writer, this.content);
    //            return;
    //        }
    writer.write('<');
    writer.write(this.name);
    if (!this.attributes.isEmpty()) {
      Enumeration<String> en = Collections.enumeration(this.attributes.keySet());
      while (en.hasMoreElements()) {
        writer.write(' ');
        String key = en.nextElement();
        String value = this.attributes.get(key);
        writer.write(key);
        writer.write('=');
        writer.write('"');
        writeEncoded(writer, value);
        writer.write('"');
      }
    }
    if (!content.isEmpty()) {
      writer.write('>');
      writeEncoded(writer, this.content);
      writer.write('<');
      writer.write('/');
      writer.write(this.name);
      writer.write('>');
    } else if (children.isEmpty()) {
      writer.write('/');
      writer.write('>');
    } else {
      writer.write('>');
      for (XmlElement child : children) {
        child.write(writer);
      }
      writer.write('<');
      writer.write('/');
      writer.write(this.name);
      writer.write('>');
    }
  }

  private static void writeEncoded(Writer writer, String str) throws IOException {
    for (int i = 0; i < str.length(); i += 1) {
      char ch = str.charAt(i);
      switch (ch) {
        case '<':
          writer.write('&');
          writer.write('l');
          writer.write('t');
          writer.write(';');
          break;
        case '>':
          writer.write('&');
          writer.write('g');
          writer.write('t');
          writer.write(';');
          break;
        case '&':
          writer.write('&');
          writer.write('a');
          writer.write('m');
          writer.write('p');
          writer.write(';');
          break;
        case '"':
          writer.write('&');
          writer.write('q');
          writer.write('u');
          writer.write('o');
          writer.write('t');
          writer.write(';');
          break;
        case '\'':
          writer.write('&');
          writer.write('a');
          writer.write('p');
          writer.write('o');
          writer.write('s');
          writer.write(';');
          break;
        default:
          int unicode = (int) ch;
          if ((unicode < 32) || (unicode > 126)) {
            writer.write('&');
            writer.write('#');
            writer.write('x');
            writer.write(Integer.toString(unicode, 16));
            writer.write(';');
          } else {
            writer.write(ch);
          }
      }
    }
  }

  /**
   * Scans an identifier from the current reader. The scanned identifier is appended to <code>result
   * </code>.
   *
   * @param result The buffer in which the scanned identifier will be put.
   */
  private void scanIdentifier(StringBuilder result) throws IOException {
    for (; ; ) {
      char ch = this.readChar();
      if (!isValidIdentifierCharacter(ch)) {
        this.unreadChar(ch);
        return;
      }
      result.append(ch);
    }
  }

  // VisibleForTesting
  static boolean isValidIdentifierCharacter(char ch) {
    return ((ch >= 'A') && (ch <= 'Z'))
        || //
        ((ch >= 'a') && (ch <= 'z'))
        || //
        ((ch >= '0') && (ch <= '9'))
        || //
        (ch == '_')
        || //
        (ch == '.')
        || //
        (ch == ':')
        || //
        (ch == '-')
        || //
        (ch > '\u007E');
  }

  /**
   * This method scans an identifier from the current reader.
   *
   * @return the next character following the whitespace.
   */
  private char scanWhitespace() throws IOException {
    for (; ; ) {
      char ch = this.readChar();
      switch (ch) {
        case ' ':
        case '\t':
        case '\n':
        case '\r':
          break;
        default:
          return ch;
      }
    }
  }

  /**
   * This method scans an identifier from the current reader. The scanned whitespace is appended to
   * <code>result</code>.
   *
   * @return the next character following the whitespace.
   */
  private char scanWhitespace(StringBuilder result) throws IOException {
    for (; ; ) {
      char ch = this.readChar();
      switch (ch) {
        case ' ':
        case '\t':
        case '\n':
          result.append(ch);
          break;
        case '\r':
          break;
        default:
          return ch;
      }
    }
  }

  /**
   * This method scans a delimited string from the current reader. The scanned string without
   * delimiters is appended to <code>string</code>.
   */
  private void scanString(StringBuilder string) throws IOException {
    char delimiter = this.readChar();
    if ((delimiter != '\'') && (delimiter != '"')) {
      throw this.createUnexpectedInputException("' or \"");
    }
    for (; ; ) {
      char ch = this.readChar();
      if (ch == delimiter) {
        return;
      } else if (ch == '&') {
        this.resolveEntity(string);
      } else {
        string.append(ch);
      }
    }
  }

  /**
   * Scans a #PCDATA element. CDATA sections and entities are resolved. The next &lt; char is
   * skipped. The scanned data is appended to <code>data</code>.
   */
  private void scanPCData(StringBuilder data) throws IOException {
    for (; ; ) {
      char ch = this.readChar();
      if (ch == '<') {

        //              System.out.println("ch="+ ch + ", rest="+ readAll());
        //              if (true) throw new RuntimeException();
        ch = this.readChar();
        if (ch == '!') {
          this.checkCDATA(data);
        } else {
          this.unreadChar(ch);
          return;
        }
      } else if (ch == '&') {
        this.resolveEntity(data);
      } else {
        data.append(ch);
      }
    }
  }

  /**
   * Scans a special tag and if the tag is a CDATA section, append its content to <code>buf</code>.
   */
  private boolean checkCDATA(StringBuilder buf) throws IOException {
    char ch = this.readChar();
    if (ch != '[') {
      this.unreadChar(ch);
      this.skipSpecialTag(0);
      return false;
    } else if (!this.checkLiteral("CDATA[")) {
      this.skipSpecialTag(1); // one [ has already been read
      return false;
    } else {
      int delimiterCharsSkipped = 0;
      while (delimiterCharsSkipped < 3) {
        ch = this.readChar();
        switch (ch) {
          case ']':
            if (delimiterCharsSkipped < 2) {
              delimiterCharsSkipped += 1;
            } else {
              buf.append(']');
              buf.append(']');
              delimiterCharsSkipped = 0;
            }
            break;
          case '>':
            if (delimiterCharsSkipped < 2) {
              for (int i = 0; i < delimiterCharsSkipped; i++) {
                buf.append(']');
              }
              delimiterCharsSkipped = 0;
              buf.append('>');
            } else {
              delimiterCharsSkipped = 3;
            }
            break;
          default:
            for (int i = 0; i < delimiterCharsSkipped; i += 1) {
              buf.append(']');
            }
            buf.append(ch);
            delimiterCharsSkipped = 0;
        }
      }
      return true;
    }
  }

  /** Skips a comment. */
  private void skipComment() throws IOException {
    int dashesToRead = 2;
    while (dashesToRead > 0) {
      char ch = this.readChar();
      if (ch == '-') {
        dashesToRead -= 1;
      } else {
        dashesToRead = 2;
      }
    }
    if (this.readChar() != '>') {
      throw this.createUnexpectedInputException(">");
    }
  }

  /**
   * Skips a special tag or comment.
   *
   * @param bracketLevel The number of open square brackets ([) that have already been read.
   */
  private void skipSpecialTag(int bracketLevel) throws IOException {
    int tagLevel = 1; // <
    char stringDelimiter = '\0';
    if (bracketLevel == 0) {
      char ch = this.readChar();
      if (ch == '[') {
        bracketLevel += 1;
      } else if (ch == '-') {
        ch = this.readChar();
        if (ch == '[') {
          bracketLevel += 1;
        } else if (ch == ']') {
          bracketLevel -= 1;
        } else if (ch == '-') {
          this.skipComment();
          return;
        }
      }
    }
    while (tagLevel > 0) {
      char ch = this.readChar();
      if (stringDelimiter == '\0') {
        if ((ch == '"') || (ch == '\'')) {
          stringDelimiter = ch;
        } else if (bracketLevel <= 0) {
          if (ch == '<') {
            tagLevel += 1;
          } else if (ch == '>') {
            tagLevel -= 1;
          }
        }
        if (ch == '[') {
          bracketLevel += 1;
        } else if (ch == ']') {
          bracketLevel -= 1;
        }
      } else {
        if (ch == stringDelimiter) {
          stringDelimiter = '\0';
        }
      }
    }
  }

  /**
   * Scans the data for literal text. Scanning stops when a character does not match or after the
   * complete text has been checked, whichever comes first.
   *
   * @param literal the literal to check.
   */
  private boolean checkLiteral(String literal) throws IOException {
    int length = literal.length();
    for (int i = 0; i < length; i += 1) {
      if (this.readChar() != literal.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  /** Reads a character from a reader. */
  private char readChar() throws IOException {
    if (this.charReadTooMuch != '\0') {
      char ch = this.charReadTooMuch;
      this.charReadTooMuch = '\0';
      return ch;
    } else {
      int i = this.reader.read();
      if (i < 0) {
        throw this.createExceptionUnexpectedEndOfData();
      } else if (i == 10) {
        this.parserLineNr += 1;
        return '\n';
      } else {
        return (char) i;
      }
    }
  }

  private void scanElement(XmlElement elt) throws IOException {
    StringBuilder buf = new StringBuilder();
    this.scanIdentifier(buf);
    String name = buf.toString();
    elt.name = name;
    char ch = this.scanWhitespace();
    while ((ch != '>') && (ch != '/')) {
      buf.setLength(0);
      this.unreadChar(ch);
      this.scanIdentifier(buf);
      String key = buf.toString();
      ch = this.scanWhitespace();
      if (ch != '=') {
        throw this.createUnexpectedInputException("=");
      }
      this.unreadChar(this.scanWhitespace());
      buf.setLength(0);
      this.scanString(buf);
      elt.attributes.put(key, buf.toString());
      ch = this.scanWhitespace();
    }
    if (ch == '/') {
      ch = this.readChar();
      if (ch != '>') {
        throw this.createUnexpectedInputException(">");
      }
      return;
    }
    buf.setLength(0);
    ch = this.scanWhitespace(buf);
    if (ch != '<') {
      this.unreadChar(ch);
      this.scanPCData(buf);
    } else {
      for (; ; ) {
        ch = this.readChar();
        if (ch == '!') {
          if (this.checkCDATA(buf)) {
            this.scanPCData(buf);
            break;
          } else {
            ch = this.scanWhitespace(buf);
            if (ch != '<') {
              this.unreadChar(ch);
              this.scanPCData(buf);
              break;
            }
          }
        } else {
          if ((ch != '/') || this.ignoreLeadingAndTrailingWhitespace) {
            buf.setLength(0);
          }
          if (ch == '/') {
            this.unreadChar(ch);
          }
          break;
        }
      }
    }
    if (buf.length() == 0) {
      while (ch != '/') {
        if (ch == '!') {
          ch = this.readChar();
          if (ch != '-') {
            throw this.createUnexpectedInputException("Comment or Element");
          }
          ch = this.readChar();
          if (ch != '-') {
            throw this.createUnexpectedInputException("Comment or Element");
          }
          this.skipComment();
        } else {
          this.unreadChar(ch);
          XmlElement child = this.createAnotherElement();
          this.scanElement(child);
          elt.addChild(child);
        }
        ch = this.scanWhitespace();
        if (ch != '<') {
          throw this.createUnexpectedInputException("<");
        }
        ch = this.readChar();
      }
      this.unreadChar(ch);
    } else {
      if (this.ignoreLeadingAndTrailingWhitespace) {
        elt.content = buf.toString().trim();
      } else {
        elt.content = buf.toString();
      }
    }
    ch = this.readChar();
    if (ch != '/') {
      throw this.createUnexpectedInputException("/");
    }
    this.unreadChar(this.scanWhitespace());

    if (!this.checkLiteral(name)) {
      throw this.createUnexpectedInputException(name);
    }
    if (this.scanWhitespace() != '>') {
      throw this.createUnexpectedInputException(">");
    }
  }

  // for debugging
  //    private String readAll() {
  //        StringBuilder b = new StringBuilder();
  //        int c;
  //        try {
  //            while ((c = reader.read()) != -1) {
  //                b.append((char) c);
  //            }
  //        } catch (IOException e) {
  //            throw new UncheckedIOException(e);
  //        }
  //        return b.toString();
  //    }

  /**
   * Resolves an entity. The name of the entity is read from the reader. The value of the entity is
   * appended to <code>buf</code>.
   *
   * @param buf Where to put the entity value.
   */
  private void resolveEntity(StringBuilder buf) throws IOException {
    char ch = '\0';
    StringBuilder keyBuf = new StringBuilder();
    for (; ; ) {
      ch = this.readChar();
      if (ch == ';') {
        break;
      }
      keyBuf.append(ch);
    }
    String key = keyBuf.toString();
    if (key.charAt(0) == '#') {
      try {
        if (key.charAt(1) == 'x') {
          ch = (char) Integer.parseInt(key.substring(2), 16);
        } else {
          ch = (char) Integer.parseInt(key.substring(1), 10);
        }
      } catch (NumberFormatException e) {
        e.getCause();
        throw this.createExceptionUnknownEntity(key);
      }
      buf.append(ch);
    } else {
      char[] value = ENTITIES.get(key);
      if (value == null) {
        throw this.createExceptionUnknownEntity(key);
      }
      buf.append(value);
    }
  }

  /**
   * Pushes a character back to the read-back buffer.
   *
   * @param ch The character to push back.
   */
  private void unreadChar(char ch) {
    this.charReadTooMuch = ch;
  }

  /** Creates a parse exception for when the end of the data input has been reached. */
  private XmlParseException createExceptionUnexpectedEndOfData() {
    String msg = "Unexpected end of data reached";
    return new XmlParseException(this.name(), this.parserLineNr, msg);
  }

  /**
   * Creates a parse exception for when the next character read is not the character that was
   * expected.
   *
   * @param charSet The set of characters (in human readable form) that was expected.
   */
  private XmlParseException createUnexpectedInputException(String charSet) {
    String msg = "Expected: " + charSet;
    return new XmlParseException(this.name(), this.parserLineNr, msg);
  }

  private XmlParseException createExceptionUnknownEntity(String name) {
    String msg = "Unknown or invalid entity: &" + name + ";";
    return new XmlParseException(this.name(), this.parserLineNr, msg);
  }

  private static Map<String, char[]> createEntities() {
    Map<String, char[]> map = new HashMap<>();
    map.put("amp", new char[] {'&'});
    map.put("quot", new char[] {'"'});
    map.put("apos", new char[] {'\''});
    map.put("lt", new char[] {'<'});
    map.put("gt", new char[] {'>'});
    return map;
  }
}
