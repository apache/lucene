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

package org.apache.lucene.luke.app.desktop.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import javax.swing.JTextArea;

/** PrintStream for text areas */
public final class TextAreaPrintStream extends PrintStream {

  private final JTextArea textArea;

  public TextAreaPrintStream(JTextArea textArea) {
    super(
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            textArea.append(String.valueOf((char) b));
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            String s = new String(b, off, len, StandardCharsets.UTF_8);
            textArea.append(s);
          }
        },
        false,
        StandardCharsets.UTF_8);

    this.textArea = textArea;
  }

  @Override
  public void flush() {
    textArea.repaint(); // Optional: Repaint the text area after appending text
    clear();
  }

  public void clear() {
    textArea.setText("");
  }
}
