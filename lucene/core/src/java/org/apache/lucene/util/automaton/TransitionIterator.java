package org.apache.lucene.util.automaton;

public interface TransitionIterator {

    int initTransition(int state, Transition t);

    void getNextTransition(Transition t);
}
