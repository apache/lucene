package org.apache.lucene.util.automaton;

public interface ByteRunnable {

    enum TYPE {
        NFA, // use NFARunAutomaton
        DFA // use ByteRunAutomaton
    }

    /**
     * Returns the state obtained by reading the given char from the given state. Returns -1 if not
     * obtaining any such state.
     * @param state the last state
     * @param c the input codepoint
     * @return the next state, -1 if no such transaction
     */
    int step(int state, int c);

    /** Returns acceptance status for given state.
     * @param state the state
     * @return whether the state is accepted
     */
    boolean isAccept(int state);

    /**
     * Returns number of states this automaton has, note this may not be an accurate number
     * in case of NFA
     * @return number of states
     */
    int getSize();

    /** Returns true if the given byte array is accepted by this automaton */
    default boolean run(byte[] s, int offset, int length) {
        int p = 0;
        int l = offset + length;
        for (int i = offset; i < l; i++) {
            p = step(p, s[i] & 0xFF);
            if (p == -1) return false;
        }
        return isAccept(p);
    }
}
