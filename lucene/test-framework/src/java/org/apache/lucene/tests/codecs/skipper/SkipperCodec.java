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
package org.apache.lucene.tests.codecs.skipper;

import java.util.Random;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.tests.util.TestUtil;

/**
 * A codec that uses {@link Lucene90DocValuesFormat} with a random skip index size for its doc
 * values and delegates to the default codec for everything else.
 */
public final class SkipperCodec extends FilterCodec {

  /** Create a random instance. */
  public static SkipperCodec randomInstance(Random random) {
    return switch (random.nextInt(10)) {
      case 0 -> new SkipperCodec();
      default -> new SkipperCodec(random);
    };
  }

  private final DocValuesFormat docValuesFormat;
  private final int skipIndexIntervalSize;

  /** no arg constructor */
  public SkipperCodec() {
    super("SkipperCodec", TestUtil.getDefaultCodec());
    this.docValuesFormat = new Lucene90DocValuesFormat();
    skipIndexIntervalSize = -1; // default
  }

  /** Creates a DocValuesFormat with random skipIndexIntervalSize */
  public SkipperCodec(Random random) {
    super("SkipperCodec", TestUtil.getDefaultCodec());
    this.skipIndexIntervalSize = random.nextInt(1 << 3, 1 << 10);
    this.docValuesFormat = new Lucene90DocValuesFormat(skipIndexIntervalSize);
  }

  @Override
  public DocValuesFormat docValuesFormat() {
    return docValuesFormat;
  }

  @Override
  public String toString() {
    return getName() + "(skipIndexIntervalSize= " + skipIndexIntervalSize + ")";
  }
}
