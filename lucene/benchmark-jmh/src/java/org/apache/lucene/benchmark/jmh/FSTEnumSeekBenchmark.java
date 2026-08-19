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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Benchmark seek-heavy FST enum operations that exercise rewindPrefix. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class FSTEnumSeekBenchmark {

  @Param({"50000"})
  int termCount;

  @Param({"20000"})
  int queryCount;

  @Param({"clustered", "uuidPrefix"})
  String termDistribution;

  @Param({"exactHit", "nearMiss", "farMiss"})
  String queryMode;

  private BytesRef[] queries;
  private BytesRefFSTEnum<Long> bytesRefEnum;
  private int seekCeilCursor;
  private int seekFloorCursor;
  private int seekExactCursor;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    List<BytesRef> terms = buildTerms(termCount, termDistribution);
    FST<Long> fst = buildFst(terms);
    this.bytesRefEnum = new BytesRefFSTEnum<>(fst);
    this.queries = buildQueries(terms, queryCount, queryMode);
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    seekCeilCursor = 0;
    seekFloorCursor = 0;
    seekExactCursor = 0;
  }

  @Benchmark
  public void seekCeil(Blackhole blackhole) throws IOException {
    BytesRef target = queries[seekCeilCursor++ % queries.length];
    BytesRefFSTEnum.InputOutput<Long> result = bytesRefEnum.seekCeil(target);
    blackhole.consume(result == null ? null : result.output);
  }

  @Benchmark
  public void seekFloor(Blackhole blackhole) throws IOException {
    BytesRef target = queries[seekFloorCursor++ % queries.length];
    BytesRefFSTEnum.InputOutput<Long> result = bytesRefEnum.seekFloor(target);
    blackhole.consume(result == null ? null : result.output);
  }

  @Benchmark
  public void seekExact(Blackhole blackhole) throws IOException {
    BytesRef target = queries[seekExactCursor++ % queries.length];
    BytesRefFSTEnum.InputOutput<Long> result = bytesRefEnum.seekExact(target);
    blackhole.consume(result == null ? null : result.output);
  }

  private static List<BytesRef> buildTerms(int count, String distribution) {
    if ("uuidPrefix".equals(distribution)) {
      return buildUuidPrefixTerms(count);
    }
    List<BytesRef> terms = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      // This keeps long shared prefixes while preserving strict lexical ordering.
      String term = String.format(Locale.ROOT, "p%05d-s%03d", i / 100, i % 100);
      terms.add(new BytesRef(term));
    }
    return terms;
  }

  private static List<BytesRef> buildUuidPrefixTerms(int count) {
    SplittableRandom random = new SplittableRandom(0x5A17BEEFL);
    HashSet<String> seen = new HashSet<>(count * 2);
    List<BytesRef> terms = new ArrayList<>(count);
    while (terms.size() < count) {
      String uuidPrefix =
          new UUID(random.nextLong(), random.nextLong())
              .toString()
              .replace("-", "")
              .substring(0, 16);
      if (seen.add(uuidPrefix)) {
        terms.add(new BytesRef(uuidPrefix));
      }
    }
    terms.sort(Comparator.naturalOrder());
    return terms;
  }

  private static FST<Long> buildFst(List<BytesRef> terms) throws IOException {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    FSTCompiler<Long> compiler = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs).build();
    IntsRefBuilder scratch = new IntsRefBuilder();
    long output = 1;
    for (BytesRef term : terms) {
      compiler.add(Util.toIntsRef(term, scratch), output++);
    }
    FST.FSTMetadata<Long> metadata = compiler.compile();
    if (metadata == null) {
      throw new IllegalStateException("Unexpected empty FST");
    }
    return FST.fromFSTReader(metadata, compiler.getFSTReader());
  }

  private static BytesRef[] buildQueries(List<BytesRef> terms, int count, String mode) {
    SplittableRandom random = new SplittableRandom(0x5EED5EEDL);
    BytesRef[] result = new BytesRef[count];
    for (int i = 0; i < count; i++) {
      BytesRef term = terms.get(random.nextInt(terms.size()));
      result[i] =
          switch (mode) {
            case "exactHit" -> BytesRef.deepCopyOf(term);
            case "nearMiss" -> mutateLastByte(term);
            case "farMiss" -> new BytesRef(("z" + i).getBytes(StandardCharsets.UTF_8));
            default -> throw new IllegalArgumentException("unknown query mode: " + mode);
          };
    }
    return result;
  }

  private static BytesRef mutateLastByte(BytesRef term) {
    byte[] copy = new byte[term.length];
    System.arraycopy(term.bytes, term.offset, copy, 0, term.length);
    copy[copy.length - 1] = (byte) (copy[copy.length - 1] + 1);
    return new BytesRef(copy);
  }
}
