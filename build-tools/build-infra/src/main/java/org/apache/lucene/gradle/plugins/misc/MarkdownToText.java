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
package org.apache.lucene.gradle.plugins.misc;

import com.vladsch.flexmark.ast.BlockQuote;
import com.vladsch.flexmark.ast.FencedCodeBlock;
import com.vladsch.flexmark.ast.HardLineBreak;
import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.ast.IndentedCodeBlock;
import com.vladsch.flexmark.ast.Link;
import com.vladsch.flexmark.ast.LinkNodeBase;
import com.vladsch.flexmark.ast.ListBlock;
import com.vladsch.flexmark.ast.OrderedList;
import com.vladsch.flexmark.ast.Paragraph;
import com.vladsch.flexmark.ast.SoftLineBreak;
import com.vladsch.flexmark.ast.Text;
import com.vladsch.flexmark.ast.ThematicBreak;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Node;
import com.vladsch.flexmark.util.sequence.Escaping;

/** Renders markdown as plain text for console output (flexmark AST walk, paragraphs wrapped). */
final class MarkdownToText {
  private static final int WIDTH = 100;
  private final StringBuilder out = new StringBuilder();

  static String render(String markdown) {
    var r = new MarkdownToText();
    r.blocks(Parser.builder().build().parse(markdown), "", true);
    return r.out.toString().stripTrailing() + "\n";
  }

  private void blocks(Node parent, String indent, boolean loose) {
    for (Node n = parent.getFirstChild(); n != null; n = n.getNext()) {
      if (loose && n != parent.getFirstChild()) out.append('\n');
      block(n, indent);
    }
  }

  private void block(Node n, String indent) {
    switch (n) {
      case Heading h -> {
        String text = inlines(h);
        line(indent, text);
        if (h.getLevel() <= 2) line(indent, (h.getLevel() == 1 ? "=" : "-").repeat(text.length()));
      }
      case Paragraph p -> wrap(indent, inlines(p));
      case FencedCodeBlock c ->
          c.getContentChars().toString().lines().forEach(l -> line(indent + "  ", l));
      case IndentedCodeBlock c ->
          c.getContentChars().toString().lines().forEach(l -> line(indent + "  ", l));
      case BlockQuote q -> blocks(q, indent + "    ", true);
      case ThematicBreak _ -> line(indent, "-".repeat(WIDTH - indent.length()));
      case ListBlock l -> {
        int num = l instanceof OrderedList ol ? ol.getStartNumber() : 0;
        for (Node item = l.getFirstChild(); item != null; item = item.getNext(), num++) {
          if (l.isLoose() && item != l.getFirstChild()) out.append('\n');
          String marker = l instanceof OrderedList ? num + ". " : "- ";
          int start = out.length() + indent.length();
          blocks(item, indent + " ".repeat(marker.length()), l.isLoose());
          if (out.length() > start) out.replace(start, start + marker.length(), marker);
          else line(indent, marker.stripTrailing());
        }
      }
      default -> n.getChars().toString().lines().forEach(l -> line(indent, l));
    }
  }

  private String inlines(Node parent) {
    var sb = new StringBuilder();
    for (Node n = parent.getFirstChild(); n != null; n = n.getNext()) {
      switch (n) {
        case Link l -> {
          String text = inlines(l), url = l.getUrl().toString();
          sb.append(text.equals(url) ? url : text + " (" + url + ")");
        }
        case LinkNodeBase l -> sb.append(l.getUrl());
        case SoftLineBreak _ -> sb.append(' ');
        case HardLineBreak _ -> sb.append('\n');
        case Text t -> sb.append(Escaping.unescapeString(t.getChars().toString()));
        default -> sb.append(n.getChars());
      }
    }
    return sb.toString();
  }

  private void wrap(String indent, String text) {
    for (String para : text.split("\n")) {
      var sb = new StringBuilder();
      for (String word : para.split("\\s+")) {
        if (!sb.isEmpty() && indent.length() + sb.length() + 1 + word.length() > WIDTH) {
          line(indent, sb.toString());
          sb.setLength(0);
        }
        sb.append(sb.isEmpty() ? "" : " ").append(word);
      }
      line(indent, sb.toString());
    }
  }

  private void line(String indent, String s) {
    out.append(s.isEmpty() ? "" : indent).append(s).append('\n');
  }
}
