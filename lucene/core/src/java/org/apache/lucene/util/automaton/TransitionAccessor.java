package org.apache.lucene.util.automaton;

public interface TransitionAccessor {

    int initTransition(int state, Transition t);

    void getNextTransition(Transition t);

    int getNumTransitions(int state);

    /**
     * Fill the provided {@link Transition} with the index'th transition leaving the specified state.
     */
    void getTransition(int state, int index, Transition t);
}
