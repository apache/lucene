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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;

public class SynonymImpactsSource implements ImpactsSource {

  private final ImpactsEnum[] impactsEnums;
  private final Impacts[] impacts;
  private final float[] boosts;
  private Impacts lead;

  public SynonymImpactsSource(ImpactsEnum[] impactsEnums, float[] boosts) {
    this.impactsEnums = impactsEnums;
    this.boosts = boosts;
    this.impacts = new Impacts[impactsEnums.length];
  }

  @Override
  public Impacts getImpacts() throws IOException {
    // Use the impacts that have the lower next boundary as a lead.
    // It will decide on the number of levels and the block boundaries.
    if (lead == null) {
      Impacts tmpLead = null;
      for (int i = 0; i < impactsEnums.length; ++i) {
        impacts[i] = impactsEnums[i].getImpacts();
        if (tmpLead == null || impacts[i].getDocIdUpTo(0) < tmpLead.getDocIdUpTo(0)) {
          tmpLead = impacts[i];
        }
      }
      lead = tmpLead;
    }
    return new Impacts() {

      @Override
      public int numLevels() {
        // Delegate to the lead
        return lead.numLevels();
      }

      @Override
      public int getDocIdUpTo(int level) {
        // Delegate to the lead
        return lead.getDocIdUpTo(level);
      }

      @Override
      public List<Impact> getImpacts(int level) {
        final int docIdUpTo = getDocIdUpTo(level);
        return ImpactsMergingUtils.mergeImpactsPerField(
            impactsEnums, impacts, boosts, docIdUpTo, false);
      }
    };
  }

  @Override
  public void advanceShallow(int target) throws IOException {
    for (ImpactsEnum impactsEnum : impactsEnums) {
      if (impactsEnum.docID() < target) {
        impactsEnum.advanceShallow(target);
      }
    }
  }

  public Impacts[] impacts() {
    return impacts;
  }
}
