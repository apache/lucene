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

package org.apache.lucene.monitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

/**
 * Calculates the weight of a {@link Term}
 *
 * <p>Higher weights mark terms that are more useful for selecting queries, typically rarer or
 * longer ones. {@link #applyNegatedAsDouble(Term)} weighs the absence of a term, for negated
 * clauses.
 */
public interface TermWeightor extends ToDoubleFunction<Term> {

  /** A default TermWeightor based on token length */
  TermWeightor DEFAULT = lengthWeightor(3, 0.3f);

  /** The smallest weight returned by the default {@link #applyNegatedAsDouble(Term)} */
  double MIN_NEGATED_WEIGHT = 1e-6;

  /**
   * The weight of "the document does not contain {@code term}", used for negated clauses
   *
   * <p>By default the weight {@code w} of the term is read as surprisal, {@code w = -ln p} where
   * {@code p} is the share of documents containing the term, giving {@code -ln(1 - e^-w)}: rare
   * terms are weak exclusions and common terms strong ones. Override this if weights express a
   * preference rather than rarity.
   */
  default double applyNegatedAsDouble(Term term) {
    return Math.max(MIN_NEGATED_WEIGHT, -Math.log1p(-Math.exp(-applyAsDouble(term))));
  }

  /**
   * Creates a TermWeightor from separate weightings for the presence and the absence of a term
   *
   * @param weightor the weight of a term
   * @param negatedWeightor the weight of the absence of a term
   */
  static TermWeightor of(ToDoubleFunction<Term> weightor, ToDoubleFunction<Term> negatedWeightor) {
    return new TermWeightor() {
      @Override
      public double applyAsDouble(Term term) {
        return weightor.applyAsDouble(term);
      }

      @Override
      public double applyNegatedAsDouble(Term term) {
        return negatedWeightor.applyAsDouble(term);
      }
    };
  }

  /** Combine weightors by multiplication, of both their weights and their negated weights */
  static TermWeightor combine(TermWeightor... weightors) {
    return of(
        value -> {
          double r = 1;
          for (TermWeightor w : weightors) {
            r *= w.applyAsDouble(value);
          }
          return r;
        },
        value -> {
          double r = 1;
          for (TermWeightor w : weightors) {
            r *= w.applyNegatedAsDouble(value);
          }
          return r;
        });
  }

  private static TermWeightor factor(ToDoubleFunction<Term> factor) {
    return of(factor, factor);
  }

  /**
   * QueryTerms with a field from the selected set will be assigned the given weight, as both their
   * weight and their negated weight
   */
  static TermWeightor fieldWeightor(double weight, Set<String> fields) {
    return factor(
        value -> {
          if (fields.contains(value.field())) {
            return weight;
          }
          return 1;
        });
  }

  /** QueryTerms with a field from the selected set will be assigned the given weight */
  static TermWeightor fieldWeightor(double weight, String... fields) {
    return fieldWeightor(weight, new HashSet<>(Arrays.asList(fields)));
  }

  /**
   * QueryTerms with a term value from the selected set will be assigned the given weight, as both
   * their weight and their negated weight
   */
  static TermWeightor termWeightor(double weight, Set<BytesRef> terms) {
    return factor(
        value -> {
          if (terms.contains(value.bytes())) {
            return weight;
          }
          return 1;
        });
  }

  /** QueryTerms with a term value from the selected set will be assigned the given weight */
  static TermWeightor termWeightor(double weight, BytesRef... terms) {
    return termWeightor(weight, new HashSet<>(Arrays.asList(terms)));
  }

  /**
   * QueryTerms with a term and field value from the selected set will be assigned the given weight,
   * as both their weight and their negated weight
   */
  static TermWeightor termAndFieldWeightor(double weight, Set<Term> terms) {
    return factor(
        value -> {
          if (terms.contains(value)) {
            return weight;
          }
          return 1;
        });
  }

  /**
   * QueryTerms with a term and field value from the selected set will be assigned the given weight
   */
  static TermWeightor termAndFieldWeightor(double weight, Term... terms) {
    return termAndFieldWeightor(weight, new HashSet<>(Arrays.asList(terms)));
  }

  /**
   * QueryTerms will be assigned a weight based on their term frequency
   *
   * <p>More infrequent terms are weighted higher. Terms are weighted according to the function
   * {@code w = (n / freq) + k}. Terms with no associated frequency receive a weight of value {@code
   * 1}
   *
   * @param frequencies a map of terms to frequencies
   * @param n a scaling factor
   * @param k the minimum weight to scale to
   */
  static TermWeightor termFreqWeightor(Map<String, Integer> frequencies, double n, double k) {
    return value -> {
      Integer mapVal = frequencies.get(value.text());
      if (mapVal != null) return (n / mapVal) + k;
      return 1;
    };
  }

  /**
   * QueryTerms will be assigned a weight based on their term length
   *
   * <p>Weights are assigned by the function {@code a * e ^ (-k * length)}. Longer terms are
   * weighted higher. Terms of length greater than 32 all receive the same weight.
   *
   * @param a a
   * @param k k
   */
  static TermWeightor lengthWeightor(double a, double k) {
    final double[] lengthNorms = new double[32];
    for (int i = 0; i < 32; i++) {
      lengthNorms[i] = (float) (a * (Math.exp(-k * i)));
    }
    return value -> {
      if (value.bytes().length >= 32) {
        return 4 - lengthNorms[31];
      }
      return 4 - lengthNorms[value.bytes().length];
    };
  }
}
