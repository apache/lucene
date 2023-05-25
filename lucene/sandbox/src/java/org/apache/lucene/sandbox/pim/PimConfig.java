package org.apache.lucene.sandbox.pim;

/**
 * PIM system configuration
 */
public class PimConfig {

  int nbDpus;

  public PimConfig() { this(2); }
  public PimConfig(int nbDpus) {
    this.nbDpus = nbDpus;
  }
  public int getNumDpus() {
    return nbDpus;
  }
}
