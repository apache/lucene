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
package org.apache.lucene.tests.search.similarities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.AxiomaticF1EXP;
import org.apache.lucene.search.similarities.AxiomaticF1LOG;
import org.apache.lucene.search.similarities.AxiomaticF2EXP;
import org.apache.lucene.search.similarities.AxiomaticF2LOG;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Independence;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.IndependenceSaturated;
import org.apache.lucene.search.similarities.IndependenceStandardized;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Similarity implementation that randomizes Similarity implementations per-field.
 *
 * <p>The choices are 'sticky', so the selected algorithm is always used for the same field.
 */
public class RandomSimilarity extends PerFieldSimilarityWrapper {
  private final List<Similarity> knownSims;
  private final Map<String, Similarity> previousMappings = new HashMap<>();
  private final int perFieldSeed;
  private final boolean shouldQueryNorm;

  public RandomSimilarity(Random random) {
    perFieldSeed = random.nextInt();
    shouldQueryNorm = random.nextBoolean();
    knownSims = new ArrayList<>(allSims);
    Collections.shuffle(knownSims, random);
  }

  @Override
  public synchronized Similarity get(String field) {
    assert field != null;
    return previousMappings.computeIfAbsent(
        field,
        f -> knownSims.get(Math.max(0, Math.abs(perFieldSeed ^ f.hashCode())) % knownSims.size()));
  }

  // all the similarities that we rotate through
  /** The DFR basic models to test. */
  private static final BasicModel[] BASIC_MODELS = {
    new BasicModelG(), new BasicModelIF(), new BasicModelIn(), new BasicModelIne(),
  };

  /** The DFR aftereffects to test. */
  private static final AfterEffect[] AFTER_EFFECTS = {new AfterEffectB(), new AfterEffectL()};

  /** The DFR normalizations to test. */
  private static final Normalization[] NORMALIZATIONS = {
    new NormalizationH1(), new NormalizationH2(),
    new NormalizationH3(), new NormalizationZ()
    // TODO: if we enable NoNormalization, we have to deal with
    // a couple tests (e.g. TestDocBoost, TestSort) that expect length normalization
    // new Normalization.NoNormalization()
  };

  /** The distributions for IB. */
  private static final Distribution[] DISTRIBUTIONS = {new DistributionLL(), new DistributionSPL()};

  /** Lambdas for IB. */
  private static final Lambda[] LAMBDAS = {new LambdaDF(), new LambdaTTF()};

  /** Independence measures for DFI */
  private static final Independence[] INDEPENDENCE_MEASURES = {
    new IndependenceStandardized(), new IndependenceSaturated(), new IndependenceChiSquared()
  };

  private static final List<Similarity> allSims;

  static {
    allSims = new ArrayList<>();
    allSims.add(new ClassicSimilarity());
    allSims.add(new BM25Similarity());
    allSims.add(new AxiomaticF1EXP());
    allSims.add(new AxiomaticF1LOG());
    allSims.add(new AxiomaticF2EXP());
    allSims.add(new AxiomaticF2LOG());

    allSims.add(new BooleanSimilarity());
    for (BasicModel basicModel : BASIC_MODELS) {
      for (AfterEffect afterEffect : AFTER_EFFECTS) {
        for (Normalization normalization : NORMALIZATIONS) {
          allSims.add(new DFRSimilarity(basicModel, afterEffect, normalization));
        }
      }
    }
    for (Distribution distribution : DISTRIBUTIONS) {
      for (Lambda lambda : LAMBDAS) {
        for (Normalization normalization : NORMALIZATIONS) {
          allSims.add(new IBSimilarity(distribution, lambda, normalization));
        }
      }
    }
    allSims.add(new LMDirichletSimilarity());
    allSims.add(new LMJelinekMercerSimilarity(0.1f));
    allSims.add(new LMJelinekMercerSimilarity(0.7f));
    for (Independence independence : INDEPENDENCE_MEASURES) {
      allSims.add(new DFISimilarity(independence));
    }
  }

  @Override
  public synchronized String toString() {
    return "RandomSimilarity(queryNorm=" + shouldQueryNorm + "): " + previousMappings.toString();
  }
}
