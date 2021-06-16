package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;

/** Converts an Automaton into a TokenStream. */
public class AutomatonToTokenStream {

  private AutomatonToTokenStream() {}

  /**
   * converts an automaton into a TokenStream. This is done by first Topo sorting the nodes in the
   * Automaton. Nodes that have the same distance from the start are grouped together to form the
   * position nodes for the TokenStream. The resulting TokenStream releases edges from the automaton
   * as tokens in order from the position nodes. This requires the automaton be a deterministic DAG.
   *
   * @param automaton automaton to convert. Must be deterministic DAG.
   * @return TokenStream representing
   */
  public static TokenStream toTokenStream(Automaton automaton) {
    if (Operations.isFinite(automaton) == false) {
      throw new IllegalArgumentException("Automaton must be finite");
    }

    List<List<Integer>> positionNodes = new ArrayList<>();

    Transition[][] transitions = automaton.getSortedTransitions();

    List<Set<Integer>> incomingEdges = new ArrayList<>(transitions.length);
    for (int i = 0; i < transitions.length; i++) {
      incomingEdges.add(new HashSet<>());
    }
    for (int i = 0; i < transitions.length; i++) {
      for (int edge = 0; edge < transitions[i].length; edge++) {
        incomingEdges.get(transitions[i][edge].dest).add(i);
      }
    }
    if (incomingEdges.get(0).isEmpty() == false) {
      throw new IllegalArgumentException("Start node has incoming edges, creating cycle");
    }

    LinkedList<RemapNode> noInputs = new LinkedList<>();
    Map<Integer, Integer> idToPos = new HashMap<>();
    noInputs.addLast(new RemapNode(0, 0));
    while (noInputs.size() > 0) {
      RemapNode currState = noInputs.removeFirst();
      for (int i = 0; i < transitions[currState.id].length; i++) {
        Set<Integer> destInputs = incomingEdges.get(transitions[currState.id][i].dest);
        destInputs.remove(currState.id);
        if (destInputs.isEmpty()) {
          noInputs.addLast(new RemapNode(transitions[currState.id][i].dest, currState.pos + 1));
        }
      }
      if (positionNodes.size() == currState.pos) {
        List<Integer> posIncs = new ArrayList<>();
        posIncs.add(currState.id);
        positionNodes.add(posIncs);
      } else {
        positionNodes.get(currState.pos).add(currState.id);
      }
      idToPos.put(currState.id, currState.pos);
    }

    for (Set<Integer> edges : incomingEdges) {
      if (edges.isEmpty() == false) {
        throw new IllegalArgumentException("Cycle found in automaton");
      }
    }

    List<List<EdgeToken>> edgesByLayer = new ArrayList<>();
    for (List<Integer> layer : positionNodes) {
      List<EdgeToken> edges = new ArrayList<>();
      for (int state : layer) {
        for (Transition t : transitions[state]) {
          edges.add(new EdgeToken(idToPos.get(t.dest), t.min));
        }
      }
      edgesByLayer.add(edges);
    }

    return new TopoTokenStream(edgesByLayer);
  }

  private static class TopoTokenStream extends TokenStream {

    private final List<List<EdgeToken>> edgesByPos;
    private int currentPos;
    private int currentEdgeIndex;
    private CharTermAttribute charAttr = addAttribute(CharTermAttribute.class);
    private PositionIncrementAttribute incAttr = addAttribute(PositionIncrementAttribute.class);
    private PositionLengthAttribute lenAttr = addAttribute(PositionLengthAttribute.class);
    private OffsetAttribute offAttr = addAttribute(OffsetAttribute.class);

    public TopoTokenStream(List<List<EdgeToken>> edgesByPos) {
      this.edgesByPos = edgesByPos;
    }

    @Override
    public boolean incrementToken() throws IOException {
      clearAttributes();
      while (currentPos < edgesByPos.size()
          && currentEdgeIndex == edgesByPos.get(currentPos).size()) {
        currentEdgeIndex = 0;
        currentPos += 1;
      }
      if (currentPos == edgesByPos.size()) {
        return false;
      }
      EdgeToken currentEdge = edgesByPos.get(currentPos).get(currentEdgeIndex);

      charAttr.append((char) currentEdge.value);

      incAttr.setPositionIncrement(currentEdgeIndex == 0 ? 1 : 0);

      lenAttr.setPositionLength(currentEdge.destination - currentPos);

      offAttr.setOffset(currentPos, currentEdge.destination);

      currentEdgeIndex++;

      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      clearAttributes();
      currentPos = 0;
      currentEdgeIndex = 0;
    }

    @Override
    public void end() throws IOException {
      clearAttributes();
      incAttr.setPositionIncrement(0);
      // -1 because we don't count the terminal state as a position in the TokenStream
      offAttr.setOffset(edgesByPos.size() - 1, edgesByPos.size() - 1);
    }
  }

  private static class EdgeToken {
    public final int destination;
    public final int value;

    public EdgeToken(int destination, int value) {
      this.destination = destination;
      this.value = value;
    }
  }

  private static class RemapNode {
    public final int id;
    public final int pos;

    public RemapNode(int id, int pos) {
      this.id = id;
      this.pos = pos;
    }
  }
}
