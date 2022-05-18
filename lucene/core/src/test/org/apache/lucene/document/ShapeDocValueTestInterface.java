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
package org.apache.lucene.document;

import java.util.ArrayList;
import java.util.List;

/**
 * Base interface for Shape Doc Values tests. Used to compute the tessellation for {@link XYShape}
 * and {@link LatLonShape}
 */
public interface ShapeDocValueTestInterface {
  Field[] getIndexableTessellation(Object shape);

  default List<ShapeField.DecodedTriangle> getTessellation(Object shape) {
    Field[] fields = getIndexableTessellation(shape);
    List<ShapeField.DecodedTriangle> tess = new ArrayList<>(fields.length);
    for (Field f : fields) {
      ShapeField.DecodedTriangle d = new ShapeField.DecodedTriangle();
      ShapeField.decodeTriangle(f.binaryValue().bytes, d);
      tess.add(d);
    }
    return tess;
  }
}
