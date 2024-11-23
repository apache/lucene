/*
 * The NanoXML 2 Lite licence blurb is included here. The classes have been
 * completely butchered but the core xml parsing routines are thanks to
 * the NanoXML authors.
 *
 **/

/* XMLParseException.java
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

package org.apache.lucene.store.s3.client.xml;

/**
 * An XMLParseException is thrown when an error occures while parsing an XML string.
 *
 * <p>Revision: 1.4 $ Date: 2002/03/24 10:27:59 $
 *
 * <p>see com.github.davidmoten.aws.lw.client.xml.XmlElement
 *
 * <p>author Marc De Scheemaecker
 *
 * @version $Name: RELEASE_2_2_1 $, Revision: 1.4 $
 */
public class XmlParseException extends RuntimeException {

  /** */
  private static final long serialVersionUID = 2719032602966457493L;

  /** Indicates that no line number has been associated with this exception. */
  public static final int NO_LINE = -1;

  /**
   * The line number in the source code where the error occurred, or <code>NO_LINE</code> if the
   * line number is unknown.
   *
   * <dl>
   *   <dt><b>Invariants:</b>
   *   <dd>
   *       <ul>
   *         <li><code>lineNumber &gt 0 || lineNumber == NO_LINE</code>
   *       </ul>
   * </dl>
   */
  private int lineNumber;

  /**
   * Creates an exception.
   *
   * @param name The name of the element where the error is located.
   * @param lineNumber The number of the line in the input.
   * @param message A message describing what went wrong.
   *     <dl>
   *       <dt><b>Preconditions:</b>
   *       <dd>
   *           <ul>
   *             <li><code>message != null</code>
   *             <li><code>lineNumber &gt; 0</code>
   *           </ul>
   *     </dl>
   *     <dl>
   *       <dt><b>Postconditions:</b>
   *       <dd>
   *           <ul>
   *             <li>getLineNumber() => lineNr
   *           </ul>
   *     </dl>
   */
  public XmlParseException(String name, int lineNumber, String message) {
    super(
        "Problem parsing "
            + ((name == null) ? "the XML definition" : ("a " + name + " element"))
            + " at line "
            + lineNumber
            + ": "
            + message);
    this.lineNumber = lineNumber;
  }

  /** Where the error occurred, or <code>NO_LINE</code> if the line number is unknown. */
  public int lineNumber() {
    return this.lineNumber;
  }
}
