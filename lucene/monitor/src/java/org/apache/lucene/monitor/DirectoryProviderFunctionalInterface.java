package org.apache.lucene.monitor;

import org.apache.lucene.store.Directory;

import java.io.IOException;

public interface DirectoryProviderFunctionalInterface {
    public Directory apply() throws IOException;

}
