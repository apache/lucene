package org.apache.lucene.sandbox.pim;

/**
 * PIM system configuration
 */
public class PimConfig {

  int nbDpus;
  int nbDpuSegments;

  public PimConfig() { this(2, 1); }
  public PimConfig(int nbDpus) { this(nbDpus, 1); }
  public PimConfig(int nbDpus, int nbDpuSegments) {

    if(nbDpuSegments <= 0 || nbDpuSegments > 255 || Integer.bitCount(nbDpuSegments) > 1)
      throw new IllegalArgumentException("Number of DPU segments should be a power of 2 between 1 and 255");

    this.nbDpus = nbDpus;
    this.nbDpuSegments = nbDpuSegments;
  }
  public int getNumDpus() {
    return nbDpus;
  }

  public int getNumDpuSegments() {
    return nbDpuSegments;
  }
}
