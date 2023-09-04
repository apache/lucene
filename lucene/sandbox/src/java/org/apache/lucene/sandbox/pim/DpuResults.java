package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.LeafSimScorer;

import java.io.IOException;

/**
 * Class used to read results coming from PIM and score them on the fly
 * Read the results directly from the byte array filled by the PIM communication
 * APIs, without copying
 */
public class DpuResults {

    private PimQuery query;
    private DpuResultsReader reader;
    private LeafSimScorer simScorer;
    private PimMatch match;

    DpuResults(PimQuery query, DpuResultsReader reader, LeafSimScorer simScorer) {
        this.query = query;
        this.reader = reader;
        this.simScorer = simScorer;
    }

    public boolean next() {
        if(reader.eof())
            return false;
        try {
            match = query.readResult(reader, simScorer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public PimMatch match() {
        return match;
    }
}
