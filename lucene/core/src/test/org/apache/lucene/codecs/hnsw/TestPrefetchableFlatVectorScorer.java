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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.junit.Test;

/**
 * Tests that {@link PrefetchableFlatVectorScorer} overrides all methods from {@link
 * FlatVectorsScorer}.
 */
public class TestPrefetchableFlatVectorScorer extends LuceneTestCase {

  @Test
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
