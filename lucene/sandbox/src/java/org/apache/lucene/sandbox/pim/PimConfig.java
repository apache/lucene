package org.apache.lucene.sandbox.pim;

public class PimConfig {

  int nbDpus;

  PimConfig() { this(2); }
  PimConfig(int nbDpus) {
    this.nbDpus = nbDpus;
  }
  public int getNumDpus() {
    return nbDpus;
  }
}
