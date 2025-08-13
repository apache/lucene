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
package org.apache.lucene.queryparser.xml;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/** Helper methods for parsing XML */
public class DOMUtils {

  public static Element getChildByTagOrFail(Element e, String name) throws ParserException {
    Element kid = getChildByTagName(e, name);
    if (null == kid) {
      throw new ParserException(e.getTagName() + " missing \"" + name + "\" child element");
    }
    return kid;
  }

  public static Element getFirstChildOrFail(Element e) throws ParserException {
    Element kid = getFirstChildElement(e);
    if (null == kid) {
      throw new ParserException(e.getTagName() + " does not contain a child element");
    }
    return kid;
  }

  public static String getAttributeOrFail(Element e, String name) throws ParserException {
    return e.getAttribute(name);
  }

  public static String getAttributeWithInheritanceOrFail(Element e, String name)
      throws ParserException {
    String v = getAttributeWithInheritance(e, name);
    if (null == v) {
      throw new ParserException(e.getTagName() + " missing \"" + name + "\" attribute");
    }
    return v;
  }

  public static String getNonBlankTextOrFail(Element e) throws ParserException {
    String v = getText(e).trim();
    if (v.isEmpty()) {
      throw new ParserException(e.getTagName() + " has no text");
    }
    return v;
  }

  /* Convenience method where there is only one child Element of a given name */
  public static Element getChildByTagName(Element e, String name) {
    for (Node kid = e.getFirstChild(); kid != null; kid = kid.getNextSibling()) {
      if ((kid.getNodeType() == Node.ELEMENT_NODE) && (name.equals(kid.getNodeName()))) {
        return (Element) kid;
      }
    }
    return null;
  }

  /**
   * Returns an attribute value from this node, or first parent node with this attribute defined
   *
   * @return A non-zero-length value if defined, otherwise null
   */
  public static String getAttributeWithInheritance(Element element, String attributeName) {
    String result = element.getAttribute(attributeName);
    if (result.isEmpty()) {
      Node n = element.getParentNode();
      if ((n == element) || (n == null)) {
        return null;
      }
      if (n instanceof Element parent) {
        return getAttributeWithInheritance(parent, attributeName);
      }
      return null; // we reached the top level of the document without finding attribute
    }
    return result;
  }

  public static String getAttribute(Element element, String attributeName, String defaultValue) {
    String result = element.getAttribute(attributeName);
    return result.isEmpty() ? defaultValue : result;
  }

  public static float getAttribute(Element element, String attributeName, float defaultValue) {
    String result = element.getAttribute(attributeName);
    return result.isEmpty() ? defaultValue : Float.parseFloat(result);
  }

  public static int getAttribute(Element element, String attributeName, int defaultValue) {
    String result = element.getAttribute(attributeName);
    return result.isEmpty() ? defaultValue : Integer.parseInt(result);
  }

  public static boolean getAttribute(Element element, String attributeName, boolean defaultValue) {
    String result = element.getAttribute(attributeName);
    return result.isEmpty() ? defaultValue : Boolean.parseBoolean(result);
  }

  /* Returns text of node and all child nodes - without markup */
  public static String getText(Node e) {
    StringBuilder sb = new StringBuilder();
    getTextBuffer(e, sb);
    return sb.toString();
  }

  public static Element getFirstChildElement(Element element) {
    for (Node kid = element.getFirstChild(); kid != null; kid = kid.getNextSibling()) {
      if (kid.getNodeType() == Node.ELEMENT_NODE) {
        return (Element) kid;
      }
    }
    return null;
  }

  private static void getTextBuffer(Node e, StringBuilder sb) {
    for (Node kid = e.getFirstChild(); kid != null; kid = kid.getNextSibling()) {
      switch (kid.getNodeType()) {
        case Node.TEXT_NODE:
          {
            sb.append(kid.getNodeValue());
            break;
          }
        case Node.ELEMENT_NODE, Node.ENTITY_REFERENCE_NODE:
          {
            getTextBuffer(kid, sb);
            break;
          }
      }
    }
  }
}
