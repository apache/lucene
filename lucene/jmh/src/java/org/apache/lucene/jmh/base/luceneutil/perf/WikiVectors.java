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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/** The type Wiki vectors. */
public class WikiVectors {

  private final VectorDictionary dict;

  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  //  /** The Dimension. */
  //  int dimension;
  // --Commented out by Inspection STOP (10/7/21, 12:37 AM)

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("usage: WikiVectors <vectorDictionary> <lineDocs> <docVectorOutput>");
      System.exit(-1);
    }
    WikiVectors wv = new WikiVectors(new VectorDictionary(args[0]));
    wv.computeVectors(args[1], args[2]);
  }

  /**
   * Instantiates a new Wiki vectors.
   *
   * @param dict the dict
   */
  WikiVectors(VectorDictionary dict) {
    this.dict = dict;
  }

  /**
   * Compute vectors.
   *
   * @param lineDocFile the line doc file
   * @param outputFile the output file
   * @throws IOException the io exception
   */
  void computeVectors(String lineDocFile, String outputFile) throws IOException {
    int count = 0;
    CharsetDecoder dec =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(
                CodingErrorAction
                    .REPLACE); // replace invalid input with the UTF8 replacement character
    try (OutputStream out = Files.newOutputStream(Paths.get(outputFile));
        Reader r = Channels.newReader(FileChannel.open(Paths.get(lineDocFile)), dec, -1);
        BufferedReader in = new BufferedReader(r)) {
      String lineDoc;
      byte[] buffer = new byte[dict.dimension * Float.BYTES];
      ByteBuffer bbuf = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
      FloatBuffer fbuf = bbuf.asFloatBuffer();
      while ((lineDoc = in.readLine()) != null) {
        float[] dvec = dict.computeTextVector(lineDoc);
        fbuf.position(0);
        fbuf.put(dvec);
        out.write(buffer);
        if (++count % 10000 == 0) {
          System.out.print("wrote " + count + "\n");
        }
      }
      System.out.println("wrote " + count);
    } catch (IOException e) {
      System.err.println("An error occurred on line " + (count + 1));
      throw e;
    }
  }
}
