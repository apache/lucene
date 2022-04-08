package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.morph.BinaryDictionary;
import org.apache.lucene.analysis.morph.ConnectionCosts;
import org.apache.lucene.analysis.morph.Dictionary;
import org.apache.lucene.analysis.morph.MorphData;
import org.apache.lucene.analysis.morph.TokenInfoFST;
import org.apache.lucene.analysis.morph.Viterbi;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;

public class ViterbiNBest extends org.apache.lucene.analysis.morph.Viterbi<Token, ViterbiNBest.PositionNBest>{

  protected ViterbiNBest(TokenInfoFST fst, FST.BytesReader fstReader, BinaryDictionary<? extends MorphData> dictionary,
                         TokenInfoFST userFST, FST.BytesReader userFSTReader, Dictionary<? extends MorphData> userDictionary,
                         ConnectionCosts costs, Class<PositionNBest> positionImpl) {
    super(fst, fstReader, dictionary, userFST, userFSTReader, userDictionary, costs, positionImpl);
  }

  @Override
  protected void processUnknownWord(boolean anyMatches, Position posData) throws IOException {

  }

  @Override
  protected void backtrace(Position endPosData, int fromIDX) {

  }

  public static class PositionNBest extends Viterbi.Position {

  }
}
