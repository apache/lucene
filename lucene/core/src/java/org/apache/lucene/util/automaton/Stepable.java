package org.apache.lucene.util.automaton;

public interface Stepable {

    /**
     * Returns the state obtained by reading the given char from the given state. Returns -1 if not
     * obtaining any such state.
     */
    int step(int state, int c);

    /** Returns acceptance status for given state. */
    boolean isAccept(int state);
}
