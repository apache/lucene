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
package org.apache.lucene.store;

import java.util.List;
import java.util.Set;
import org.apache.lucene.store.IOContext.FileOpenHint;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIOContext extends LuceneTestCase {

  static final Class<UnsupportedOperationException> UOE = UnsupportedOperationException.class;

  public void testUnmodifiable() {
    for (var context : getContexts()) {
      expectThrows(UOE, () -> context.hints().remove(randomHint()));
      expectThrows(UOE, () -> context.hints().removeAll(List.of(randomHint())));
      expectThrows(UOE, () -> context.hints().add(randomHint()));

      var prevHints = context.hints();
      var newHint = randomHint();
      var newContext = context.withHints(newHint);
      assertEquals(prevHints, context.hints());
      assertEquals(context.context(), newContext.context());
      assertEquals(context.mergeInfo(), newContext.mergeInfo());
      assertEquals(context.flushInfo(), newContext.flushInfo());
      if (context != newContext) {
        assertTrue(context.mergeInfo() == null && context.flushInfo() == null);
        assertEquals(Set.of(newHint), newContext.hints());
      } else {
        assertTrue(context.mergeInfo() != null || context.flushInfo() != null);
      }
    }
  }

  protected List<IOContext> getContexts() {
    return List.of(
        IOContext.DEFAULT,
        IOContext.READONCE,
        IOContext.flush(new FlushInfo(1, 2)),
        IOContext.merge(new MergeInfo(1, 2, true, 4)));
  }

  static FileOpenHint randomHint() {
    var values =
        switch (random().nextInt(5)) {
          case 0 -> DataAccessHint.values();
          case 1 -> FileDataHint.values();
          case 2 -> FileTypeHint.values();
          case 3 -> PreloadHint.values();
          case 4 -> CustomTestHint.values();
          default -> throw new IllegalStateException("Unexpected value");
        };
    return values[random().nextInt(values.length)];
  }

  enum CustomTestHint implements FileOpenHint {
    CUSTOM_TEST_HINT1,
    CUSTOM_TEST_HINT2
  }
}
