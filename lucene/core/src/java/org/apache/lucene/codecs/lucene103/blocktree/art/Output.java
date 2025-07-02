package org.apache.lucene.codecs.lucene103.blocktree.art;

import org.apache.lucene.util.BytesRef;

/**
 * The output describing the term block the prefix point to.
 *
 * @param fp the file pointer to the on-disk terms block which a trie node points to.
 * @param hasTerms false if this on-disk block consists entirely of pointers to child blocks.
 * @param floorData will be non-null when a large block of terms sharing a single trie prefix is
 *     split into multiple on-disk blocks.
 */
public record Output(long fp, boolean hasTerms, BytesRef floorData) {
}
