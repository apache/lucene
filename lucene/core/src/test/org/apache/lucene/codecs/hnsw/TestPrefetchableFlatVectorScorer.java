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
package org.apache.lucene.codecs.hnsw;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Tests that {@link PrefetchableFlatVectorScorer} overrides all methods from {@link
 * FlatVectorsScorer}.
 */
public class TestPrefetchableFlatVectorScorer extends LuceneTestCase {

  public void testDeclaredMethodsOverridden() {
    implTestDeclaredMethodsOverridden(FlatVectorsScorer.class, PrefetchableFlatVectorScorer.class);
    implTestDeclaredMethodsOverridden(
        RandomVectorScorerSupplier.class,
        PrefetchableFlatVectorScorer.PrefetchableRandomVectorScorerSupplier.class);
    implTestDeclaredMethodsOverridden(
        PrefetchableFlatVectorScorer.PrefetchableRandomVectorScorer.class.getSuperclass(),
        PrefetchableFlatVectorScorer.PrefetchableRandomVectorScorer.class);
    implTestDeclaredMethodsOverridden(
        PrefetchableFlatVectorScorer.PrefetchableUpdatableRandomVectorScorer.class.getSuperclass(),
        PrefetchableFlatVectorScorer.PrefetchableUpdatableRandomVectorScorer.class);
  }

  public void testUpdatableScorerDelegatesBulkScore() throws Exception {
    FloatVectorValues values =
        FloatVectorValues.fromFloats(List.of(new float[] {1f}, new float[] {2f}), 1);
    int[] bulkScoreCalls = new int[1];
    int[] scoreCalls = new int[1];
    UpdateableRandomVectorScorer delegate =
        new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
          @Override
          public void setScoringOrdinal(int node) {}

          @Override
          public float score(int node) {
            scoreCalls[0]++;
            return 0f;
          }

          @Override
          public float bulkScore(int[] nodes, float[] scores, int numNodes) {
            bulkScoreCalls[0]++;
            return 0f;
          }
        };
    new PrefetchableFlatVectorScorer.PrefetchableUpdatableRandomVectorScorer(delegate, values)
        .bulkScore(new int[] {0, 1}, new float[2], 2);
    assertEquals(1, bulkScoreCalls[0]);
    assertEquals(0, scoreCalls[0]);
  }

  private void implTestDeclaredMethodsOverridden(Class<?> interfaceClass, Class<?> implClass) {
    for (final Method superClassMethod : interfaceClass.getDeclaredMethods()) {
      final int modifiers = superClassMethod.getModifiers();
      if (Modifier.isFinal(modifiers)) continue;
      if (Modifier.isStatic(modifiers)) continue;
      try {
        final Method subClassMethod =
            implClass.getDeclaredMethod(
                superClassMethod.getName(), superClassMethod.getParameterTypes());
        assertEquals(
            "getReturnType() difference",
            superClassMethod.getReturnType(),
            subClassMethod.getReturnType());
      } catch (NoSuchMethodException _) {
        fail(implClass + " needs to override '" + superClassMethod + "'");
      }
    }
  }
}
